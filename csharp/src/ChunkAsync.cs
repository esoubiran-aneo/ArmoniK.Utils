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

public static class ChunkAsync
{
  public static async IAsyncEnumerable<T[]> AsChunksAsync<T>(this IAsyncEnumerable<T>                   enumerable,
                                                             int                                        size,
                                                             TimeSpan                                   maxDelay,
                                                             [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    var                      buffer      = Array.Empty<T>();
    var                      bufferSize  = 0;
    Exception?               error       = null;
    Task?                    timeoutTask = null;
    CancellationTokenSource? cts         = null;

    void Clean()
    {
      cts?.Cancel();
      timeoutTask?.Dispose();
      cts?.Dispose();
      timeoutTask = null;
      cts         = null;
    }

    await using var enumerator = enumerable.GetAsyncEnumerator(cancellationToken);
    var nextTask = enumerator.MoveNextAsync()
                             .AsTask();

    while (true)
    {
      using var which = timeoutTask is null
                          ? nextTask
                          : await Task.WhenAny(nextTask,
                                               timeoutTask)
                                      .ConfigureAwait(false);

      try
      {
        await which.ConfigureAwait(false);
      }
      catch (Exception e)
      {
        error = e;
        break;
      }

      if (ReferenceEquals(which,
                          timeoutTask))
      {
        Debug.Assert(bufferSize > 0);
        Array.Resize(ref buffer,
                     bufferSize);
        timeoutTask.Dispose();
        timeoutTask = null;
        yield return buffer;

        buffer     = new T[bufferSize];
        bufferSize = 0;
        continue;
      }

      Debug.Assert(ReferenceEquals(which,
                                   nextTask));
      if (!await nextTask.ConfigureAwait(false))
      {
        break;
      }

      if (bufferSize == buffer.Length)
      {
        Debug.Assert(buffer.Length < size);
        var newLength = Math.Min(Math.Max(buffer.Length + buffer.Length / 2,
                                          4),
                                 size);

        Array.Resize(ref buffer,
                     newLength);
      }

      buffer[bufferSize] =  enumerator.Current;
      bufferSize         += 1;

      if (bufferSize == buffer.Length && bufferSize >= size)
      {
        Clean();
        yield return buffer;
        buffer     = new T[bufferSize];
        bufferSize = 0;
      }
      else if (timeoutTask is null)
      {
        cts ??= CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeoutTask = Task.Delay(maxDelay,
                                 cts.Token);
      }

      try
      {
        nextTask = enumerator.MoveNextAsync()
                             .AsTask();
      }
      catch (Exception e)
      {
        error = e;
        break;
      }
    }

    if (bufferSize > 0)
    {
      Array.Resize(ref buffer,
                   bufferSize);
      yield return buffer;
    }

    Clean();

    if (error is not null)
    {
      ExceptionDispatchInfo.Capture(error)
                           .Throw();
    }
  }
}
