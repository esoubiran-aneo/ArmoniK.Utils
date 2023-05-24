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
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class ChunkTest
{
  private static IEnumerable ChunkArrayCases(IEnumerable<int> chunkSizes)
  {
    foreach (var chunkSize in chunkSizes)
    {
      for (var n = 0; n <= 4 * chunkSize; ++n)
      {
        yield return new TestCaseData(Enumerable.Range(0,
                                                       n)
                                                .ToArray(),
                                      chunkSize).SetArgDisplayNames($"int[{n}], {chunkSize}");
      }
    }
  }

  ///////////////////////
  // Synchronous Chunk //
  ///////////////////////
  [Test]
  [TestCaseSource(nameof(ChunkArrayCases),
                  new object[]
                  {
                    new[]
                    {
                      1,
                      2,
                      3,
                      4,
                    },
                  })]
  public void CheckChunkSize(IEnumerable<int> enumerable,
                             int              chunkSize)
  {
    var lastLength = chunkSize;
    foreach (var chunk in enumerable.ToChunks(chunkSize))
    {
      var length = chunk.Length;
      Assert.That(length,
                  Is.InRange(1,
                             lastLength));
      Assert.That(chunkSize,
                  Is.AnyOf(length,
                           lastLength));
      lastLength = length;
    }
  }

  [Test]
  [TestCaseSource(nameof(ChunkArrayCases),
                  new object[]
                  {
                    new[]
                    {
                      1,
                      2,
                      3,
                      4,
                    },
                  })]
  public void ChunkShouldKeepOrder(int[] array,
                                   int   chunkSize)
  {
    var i = 0;

    foreach (var chunk in array.ToChunks(chunkSize)
                               .ToList())
    {
      foreach (var x in chunk)
      {
        Assert.That(i,
                    Is.LessThan(array.Length));
        Assert.That(x,
                    Is.EqualTo(array[i]));
        i += 1;
      }
    }
  }

  [Test]
  [TestCase(1)]
  [TestCase(2)]
  [TestCase(3)]
  [TestCase(4)]
  public void ChunkNullShouldSucceed(int chunkSize)
  {
    var chunks = (null as IEnumerable<int>).ToChunks(chunkSize);
    // ReSharper disable once PossibleMultipleEnumeration
    Assert.That(chunks,
                Is.Not.Null);
    // ReSharper disable once PossibleMultipleEnumeration
    Assert.That(chunks.Count(),
                Is.Zero);
  }

  [Test]
  [TestCase(null,
            0)]
  [TestCase(null,
            -1)]
  [TestCase(0,
            0)]
  [TestCase(0,
            -1)]
  [TestCase(1,
            0)]
  [TestCase(1,
            -1)]
  public void ChunkByZeroShouldFail(int? arraySize,
                                    int  chunkSize)
  {
    var enumerable = arraySize is not null
                       ? Enumerable.Range(0,
                                          (int)arraySize)
                       : null as IEnumerable<int>;
    Assert.Throws<ArgumentOutOfRangeException>(() => enumerable.ToChunks(chunkSize));
  }

  ////////////////////////
  // Asynchronous Chunk //
  ////////////////////////
  [Test]
  [TestCaseSource(nameof(ChunkArrayCases),
                  new object[]
                  {
                    new[]
                    {
                      1,
                      2,
                      3,
                      4,
                    },
                  })]
  public async Task CheckChunkAsyncSize(IEnumerable<int> enumerable,
                                        int              chunkSize)
  {
    var lastLength = chunkSize;
    await foreach (var chunk in enumerable.ToAsyncEnumerable()
                                          .ToChunksAsync(chunkSize,
                                                         TimeSpan.FromMilliseconds(100)))
    {
      var length = chunk.Length;
      Assert.That(length,
                  Is.InRange(1,
                             lastLength));
      Assert.That(chunkSize,
                  Is.AnyOf(length,
                           lastLength));
      lastLength = length;
    }
  }

  [Test]
  [TestCaseSource(nameof(ChunkArrayCases),
                  new object[]
                  {
                    new[]
                    {
                      1,
                      2,
                      3,
                      4,
                    },
                  })]
  public async Task ChunkAsyncShouldKeepOrder(int[] array,
                                              int   chunkSize)
  {
    var i = 0;

    foreach (var chunk in await array.ToAsyncEnumerable()
                                     .ToChunksAsync(chunkSize,
                                                    TimeSpan.FromMilliseconds(100))
                                     .ToListAsync()
                                     .ConfigureAwait(false))
    {
      foreach (var x in chunk)
      {
        Assert.That(i,
                    Is.LessThan(array.Length));
        Assert.That(x,
                    Is.EqualTo(array[i]));
        i += 1;
      }
    }
  }

  [Test]
  [TestCase(1)]
  [TestCase(2)]
  [TestCase(3)]
  [TestCase(4)]
  public async Task ChunkAsyncNullShouldSucceed(int chunkSize)
  {
    var chunks = (null as IAsyncEnumerable<int>).ToChunksAsync(chunkSize,
                                                               TimeSpan.FromMilliseconds(100));
    // ReSharper disable once PossibleMultipleEnumeration
    Assert.That(chunks,
                Is.Not.Null);
    // ReSharper disable once PossibleMultipleEnumeration
    Assert.That(await chunks.CountAsync()
                            .ConfigureAwait(false),
                Is.Zero);
  }

  [Test]
  [TestCase(null,
            0)]
  [TestCase(null,
            -1)]
  [TestCase(0,
            0)]
  [TestCase(0,
            -1)]
  [TestCase(1,
            0)]
  [TestCase(1,
            -1)]
  public void ChunkAsyncByZeroShouldFail(int? arraySize,
                                         int  chunkSize)
  {
    var enumerable = arraySize is not null
                       ? Enumerable.Range(0,
                                          (int)arraySize)
                                   .ToAsyncEnumerable()
                       : null as IAsyncEnumerable<int>;
    Assert.ThrowsAsync<ArgumentOutOfRangeException>(() => enumerable.ToChunksAsync(chunkSize,
                                                                                   TimeSpan.FromMilliseconds(100))
                                                                    .ToListAsync()
                                                                    .AsTask());
  }


  ///////////////////////////////////////
  // Asynchronous Chunk specific tests //
  ///////////////////////////////////////
  [Test]
  public async Task ChunkAsyncWithDelayShouldSucceed()
  {
    async IAsyncEnumerable<int> Gen()
    {
      // First chunk
      await Task.Yield();
      yield return 0;
      yield return 1;
      await Task.Yield();
      yield return 2;
      await Task.Delay(10)
                .ConfigureAwait(false);
      yield return 3;

      // Second chunk
      yield return 4;
      yield return 5;
      await Task.Delay(200)
                .ConfigureAwait(false);

      // Third chunk
      yield return 6;
      await Task.Delay(200)
                .ConfigureAwait(false);

      // Fourth chunk
      yield return 7;
      yield return 8;
      yield return 9;
      yield return 10;
      await Task.Delay(200)
                .ConfigureAwait(false);

      // Fifth chunk
      yield return 11;
    }

    var chunks = await Gen()
                       .ToChunksAsync(4,
                                      TimeSpan.FromMilliseconds(100))
                       .ToListAsync()
                       .ConfigureAwait(false);

    Assert.That(chunks,
                Is.EqualTo(new List<int[]>
                           {
                             // First chunk
                             new[]
                             {
                               0,
                               1,
                               2,
                               3,
                             },
                             // Second chunk
                             new[]
                             {
                               4,
                               5,
                             },
                             // Third chunk
                             new[]
                             {
                               6,
                             },
                             // Fourth chunk
                             new[]
                             {
                               7,
                               8,
                               9,
                               10,
                             },
                             // Fifth chunk
                             new[]
                             {
                               11,
                             },
                           }));
  }

  [Test]
  [TestCase(1)]
  [TestCase(200)]
  public async Task ChunkAsyncWithThrowShouldFail(int delay)
  {
    async IAsyncEnumerable<int> Gen()
    {
      // First chunk
      yield return 0;
      yield return 1;
      yield return 2;
      yield return 3;

      // Second chunk
      yield return 4;
      yield return 5;
      await Task.Delay(delay)
                .ConfigureAwait(false);
      throw new ApplicationException("");
    }

    var chunks = Gen()
      .ToChunksAsync(4,
                     TimeSpan.FromMilliseconds(100));

    await using var enumerator = chunks.GetAsyncEnumerator();

    Assert.That(await enumerator.MoveNextAsync(),
                Is.True);
    Assert.That(enumerator.Current,
                Is.EqualTo(new[]
                           {
                             0,
                             1,
                             2,
                             3,
                           }));

    Assert.That(await enumerator.MoveNextAsync(),
                Is.True);
    Assert.That(enumerator.Current,
                Is.EqualTo(new[]
                           {
                             4,
                             5,
                           }));

    Assert.ThrowsAsync<ApplicationException>(() => enumerator.MoveNextAsync()
                                                             .AsTask());
  }

  [Test]
  public async Task ChunkAsyncWithExternalCancellationShouldFail()
  {
    async IAsyncEnumerable<int> Gen()
    {
      // First chunk
      yield return 0;
      yield return 1;
      yield return 2;
      yield return 3;

      // Second chunk
      await Task.Yield();
      throw new ApplicationException("");
    }

    var cts = new CancellationTokenSource();

    var chunks = Gen()
      .ToChunksAsync(4,
                     TimeSpan.FromMilliseconds(100),
                     cts.Token);

    await using var enumerator = chunks.GetAsyncEnumerator(CancellationToken.None);

    Assert.That(await enumerator.MoveNextAsync(),
                Is.True);
    Assert.That(enumerator.Current,
                Is.EqualTo(new[]
                           {
                             0,
                             1,
                             2,
                             3,
                           }));
    cts.Cancel();

    Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync()
                                                                   .AsTask());
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task ChunkAsyncWithInternalCancellationShouldFail(bool afterChunk)
  {
    var cts = new CancellationTokenSource();

    async IAsyncEnumerable<int> Gen()
    {
      // First chunk
      yield return 0;
      yield return 1;
      yield return 2;

      if (afterChunk)
      {
        cts.Cancel();
      }

      yield return 3;


      // Second chunk
      await Task.Yield();
      yield return 4;
      cts.Cancel();
      yield return 5;

      throw new ApplicationException("");
    }

    var chunks = Gen()
      .ToChunksAsync(4,
                     TimeSpan.FromMilliseconds(100),
                     cts.Token);

    await using var enumerator = chunks.GetAsyncEnumerator(CancellationToken.None);

    Assert.That(await enumerator.MoveNextAsync(),
                Is.True);
    Assert.That(enumerator.Current,
                Is.EqualTo(new[]
                           {
                             0,
                             1,
                             2,
                             3,
                           }));

    if (!afterChunk)
    {
      Assert.That(await enumerator.MoveNextAsync(),
                  Is.True);
      Assert.That(enumerator.Current,
                  Is.EqualTo(new[]
                             {
                               4,
                               5,
                             }));
    }

    Assert.ThrowsAsync<OperationCanceledException>(() => enumerator.MoveNextAsync()
                                                                   .AsTask());
  }
}
