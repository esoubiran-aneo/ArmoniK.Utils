﻿// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2022-2022. All rights reserved.
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using JetBrains.Annotations;

namespace ArmoniK.Utils;

/// <summary>
///   Limit the access to a function call to a single thread,
///   all the others waiting the result from the one actually doing the call.
/// </summary>
/// <typeparam name="T">Type of the return object</typeparam>
[PublicAPI]
public class ExecutionSingleizer<T> : IDisposable
{
  private Handle handle_ = new();

  /// <inheritdoc />
  public void Dispose()
    => handle_.Dispose();

  /// <summary>
  ///   Call the asynchronous function func.
  ///   If another thread is already computing func, just wait for the result.
  ///   The actual call to func is cancelled only when all callers have cancelled it.
  /// </summary>
  /// <param name="func">Function to call</param>
  /// <param name="cancellationToken">Token to cancel the call</param>
  /// <returns>
  ///   Result of func
  /// </returns>
  public async Task<T> Call(Func<CancellationToken, Task<T>> func,
                            CancellationToken                cancellationToken = default)
  {
    // Read the handle_ reference to have a stable view of it
    var currentHandle = handle_;

    // If there is no waiters, the task is complete (success or failed), and no thread is currently running it.
    // We therefore need to call func again
    if (currentHandle.Waiters == 0)
    {
      // Prepare new handle, with new cancellation token source and new task
      var cts         = new CancellationTokenSource();
      var delayedTask = new Task<Task<T>>(() => func(cts.Token));

      var newHandle = new Handle
                      {
                        CancellationTokenSource = cts,
                        // Unwrap allows the handle to have a single level Task, instead of a Task<Task<...>>
                        InnerTask = delayedTask.Unwrap(),
                        // Current thread is implicitly waiting for the task
                        Waiters = 1,
                      };

      // Try to store new handle replacing the previous one.
      // Only one thread will succeed here.
      var previousHandle = Interlocked.CompareExchange(ref handle_,
                                                       newHandle,
                                                       currentHandle);

      // Check if thread successfully replaced the handle.
      if (ReferenceEquals(previousHandle,
                          currentHandle))
      {
        // Current thread has won, the others will see this new handle.
        // We can now start the task, other threads will just wait on the result.
        delayedTask.Start();
        currentHandle = newHandle;

        // There is no need increment number of waiters as it has been initialized to 1.
      }
      else
      {
        // The handle as been replaced by another thread, so we can just wait for the task
        // in this new handle to get the result.
        // The handle created by the current thread can be destroyed as it is not used by anything.
        newHandle.Dispose();
        currentHandle = previousHandle;

        // Record current thread as waiting for the task
        Interlocked.Increment(ref currentHandle.Waiters);
      }
    }
    else
    {
      // if the task is not complete, we can just wait its result.
      // Record current thread as waiting for the task.
      Interlocked.Increment(ref currentHandle.Waiters);
    }

    var task = currentHandle.InnerTask;
    // Wait for task.
    try
    {
      // Allow for early exit.
      var tcs = new TaskCompletionSource<T>();

      // Early exit if the current cancellationToken is cancelled.
      cancellationToken.Register(() => tcs.SetCanceled());

      // Wait for either the task to finish, or the cancellation token to be cancelled.
      return await Task.WhenAny(task,
                                tcs.Task)
                       .Unwrap()
                       .ConfigureAwait(false);
    }
    finally
    {
      // Remove the current thread from the list of waiters.
      var i = Interlocked.Decrement(ref currentHandle.Waiters);

      // If the current thread was the last, we can cancel the shared token.
      if (i == 0)
      {
        // If we enter here because the task has completed without errors,
        // cancelling is a no op, therefore, we do not need to check why we went here.
        currentHandle.CancellationTokenSource.Cancel();

        // FIXME: There might be a race condition between the dispose and the cancel here.
        // ManyConcurrentExecutionShouldSucceed fails with:
        //   `System.ObjectDisposedException : The CancellationTokenSource has been disposed.`
        // As soon as we understand where it comes from, we can reenable early dispose.

        //// The task might not have finished yet. If that is the case, let the GC do the job.
        //if (currentHandle.InnerTask.IsCompleted)
        //{
        //  // Dispose of the Handle (and therefore the underlying task) here is fine:
        //  // https://devblogs.microsoft.com/pfxteam/do-i-need-to-dispose-of-tasks/
        //  currentHandle.Dispose();
        //}
      }
    }
  }

  /// <summary>
  ///   This handle stores a Task, a cancellationTokenSource, and a counter.
  ///   This needs to be a class to enable an atomic CAS.
  ///   It cannot be inlined into the parent class.
  /// </summary>
  private sealed class Handle : IDisposable
  {
    /// <summary>
    ///   Number of threads waiting for the result.
    /// </summary>
    public int Waiters;

    /// <summary>
    ///   Construct an handle that is cancelled.
    /// </summary>
    public Handle()
    {
      // Create a CancellationTokenSource that is already cancelled.
      CancellationTokenSource = new CancellationTokenSource();
      CancellationTokenSource.Cancel();

      // InnerTask is created cancelled.
      // As a cancelled task is completed, the result of this task will never be read,
      // and another task will always be created instead.
      InnerTask = Task.FromCanceled<T>(CancellationTokenSource.Token);
    }

    /// <summary>
    ///   Shared cancellation token for all the threads waiting on the task.
    /// </summary>
    public CancellationTokenSource CancellationTokenSource { get; init; }

    /// <summary>
    ///   Task that creates the result.
    /// </summary>
    public Task<T> InnerTask { get; init; }

    /// <inheritdoc />
    public void Dispose()
    {
      CancellationTokenSource.Dispose();
      InnerTask.Dispose();
    }
  }
}
