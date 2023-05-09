// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2022-2023.All rights reserved.
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

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Runtime.CompilerServices;
using System.Runtime.ExceptionServices;
using System.Threading;
using System.Threading.Tasks;

namespace ArmoniK.Utils;

internal static class Chunk
{
  // Implementation of the AsChunked function
  // Original source code : https://github.com/dotnet/runtime/blob/main/src/libraries/System.Linq/src/System/Linq/Chunk.cs
  internal static IEnumerable<TSource[]> Iterator<TSource>(IEnumerable<TSource> source,
                                                           int                  size)
  {
    using var e = source.GetEnumerator();

    var buffer = Array.Empty<TSource>();
    int bufferSize;

    {
      // first chunk
      for (bufferSize = 0; bufferSize < size && e.MoveNext(); ++bufferSize)
      {
        if (bufferSize >= buffer.Length)
        {
          var newLength = Math.Min(Math.Max(buffer.Length + buffer.Length / 2,
                                            4),
                                   size);
          Array.Resize(ref buffer,
                       newLength);
        }

        buffer[bufferSize] = e.Current;
      }
    }
    // buffer is now the right size here

    while (true) // other chunks
    {
      if (bufferSize != size) // Incomplete chunk
      {
        // chunk is not empty, and must be trimmed and return
        if (bufferSize > 0)
        {
          Array.Resize(ref buffer,
                       bufferSize);
          yield return buffer;
        }

        yield break;
      }

      yield return buffer; // chunk is complete and a new storage is required
      buffer = new TSource[size];

      for (bufferSize = 0; bufferSize < size && e.MoveNext(); ++bufferSize)
      {
        buffer[bufferSize] = e.Current;
      }
    }
  }


  internal static async IAsyncEnumerable<T[]> IteratorAsync<T>(IAsyncEnumerable<T>                        enumerable,
                                                               int                                        size,
                                                               TimeSpan                                   maxDelay,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var                      buffer      = Array.Empty<T>();
    var                      bufferSize  = 0;
    Exception?               error       = null;
    Task?                    timeoutTask = null;
    CancellationTokenSource? cts         = null;

    // Dispose both timeoutTask ans cts, before resetting to null
    void Clean()
    {
      cts?.Cancel();
      timeoutTask?.Dispose();
      cts?.Dispose();
      timeoutTask = null;
      cts         = null;
    }

    // Check if cancellation has been requested
    cancellationToken.ThrowIfCancellationRequested();

    // Manual iteration is necessary in order to take into account the timeout
    await using var enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
    var nextTask = enumerator.MoveNextAsync()
                             .AsTask();

    // Loop over the input enumerable
    while (true)
    {
      // Wait for either the next element or the timeout
      // If timeoutTask is null, no need to call WhenAny
      using var which = timeoutTask is null
                          ? nextTask
                          : await Task.WhenAny(nextTask,
                                               timeoutTask)
                                      .ConfigureAwait(false);

      // If there is an error, record the error and stop looping
      try
      {
        await which.ConfigureAwait(false);
      }
      catch (Exception e)
      {
        error = e;
        break;
      }

      // Timeout has triggered
      if (ReferenceEquals(which,
                          timeoutTask))
      {
        // If timeout task is not null, there necessarily at least one element in the buffer
        // But the chunk is not full, otherwise, it would have been yielded before
        Debug.Assert(bufferSize > 0);
        Array.Resize(ref buffer,
                     bufferSize);

        // We can dispose and reset the timeoutTask, now that it has finished
        timeoutTask.Dispose();
        timeoutTask = null;
        yield return buffer;

        // Allocate the new buffer with the previous size
        // This avoids over-allocations if the number of elements yielded decreases
        buffer     = new T[bufferSize];
        bufferSize = 0;
        continue;
      }

      // If it was not the timeoutTask, it is necessary nextTask
      Debug.Assert(ReferenceEquals(which,
                                   nextTask));
      if (!await nextTask.ConfigureAwait(false))
      {
        break;
      }

      // If there is no room in the buffer for a new element, a new allocation is required
      if (bufferSize == buffer.Length)
      {
        Debug.Assert(buffer.Length < size);
        var newLength = Math.Min(Math.Max(buffer.Length + buffer.Length / 2,
                                          4),
                                 size);

        Array.Resize(ref buffer,
                     newLength);
      }

      // Add the element to the buffer
      buffer[bufferSize] =  enumerator.Current;
      bufferSize         += 1;

      // If the chunk is full (ie: buffer full and buffer size = chunk size), yield the chunk
      if (bufferSize == buffer.Length && bufferSize >= size)
      {
        // We can clean the timeoutTask and cts for the next chunk
        Clean();

        // Yield the current buffer
        yield return buffer;

        // Reallocate a new buffer with the same size (buffer size should chunk size)
        buffer     = new T[bufferSize];
        bufferSize = 0;
      }
      else if (timeoutTask is null)
      {
        // We added a new element to the chunk, so we need to start the timeout if not already started
        cts ??= CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutTask = Task.Delay(maxDelay,
                                 cts.Token);
      }

      // Fetch the next element
      // This can throw before being awaited, so if that is the case, we need to stop the loop
      try
      {
        // Check if cancellation has been requested
        cancellationToken.ThrowIfCancellationRequested();
        nextTask = enumerator.MoveNextAsync()
                             .AsTask();
      }
      catch (Exception e)
      {
        error = e;
        break;
      }
    }

    // If the chunk is not empty, it must be yield, even if there is an error
    if (bufferSize > 0)
    {
      // The chunk is necessarily not full because it would have been yielded otherwise
      Array.Resize(ref buffer,
                   bufferSize);
      yield return buffer;
    }

    // We can clean the timeoutTask and cts as it is not needed anymore
    Clean();

    // If there were any error, the error must be rethrow
    if (error is not null)
    {
      // Keep the stack trace for the rethrown exception
      ExceptionDispatchInfo.Capture(error)
                           .Throw();
    }
  }
}
