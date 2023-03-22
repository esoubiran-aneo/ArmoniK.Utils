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
using System.Runtime.CompilerServices;
using System.Threading;
using System.Threading.Tasks;

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class ParallelSelectExtTest
{
  private static readonly bool[] Booleans =
  {
    false,
    true,
  };

  private static IEnumerable ParallelSelectCases()
  {
    var parallelisms = new[]
                       {
                         -1,
                         0,
                         1,
                         2,
                       };
    var sizes = new[]
                {
                  0,
                  1,
                  4,
                };

    TestCaseData Case(bool useAsync,
                      int  parallelism,
                      int  size)
      => new TestCaseData(useAsync,
                          parallelism,
                          size).SetArgDisplayNames($"int[{size}], {parallelism}{(useAsync ? ", async" : "")}");

    foreach (var useAsync in Booleans)
    {
      foreach (var parallelism in parallelisms)
      {
        foreach (var size in sizes)
        {
          yield return Case(useAsync,
                            parallelism,
                            size);
        }
      }

      yield return Case(useAsync,
                        -1,
                        1000);
      yield return Case(useAsync,
                        0,
                        100);
      yield return Case(useAsync,
                        1,
                        10);
      yield return Case(useAsync,
                        1,
                        20);
    }
  }

  [Test]
  [TestCaseSource(nameof(ParallelSelectCases))]
  public async Task ParallelSelectShouldSucceed(bool useAsync,
                                                int  parallelism,
                                                int  n)
  {
    var f = AsyncIdentity(0,
                          10);
    var enumerable = useAsync
                       ? GenerateIntsAsync(n)
                         .ParallelSelect(new ParallelTaskOptions(parallelism),
                                         f)
                       : GenerateInts(n)
                         .ParallelSelect(new ParallelTaskOptions(parallelism),
                                         f);
    var x = await enumerable.ToListAsync()
                            .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));
  }

  private static IEnumerable ParallelSelectLimitCases()
  {
    TestCaseData Case(bool useAsync,
                      int  parallelism,
                      int  size)
      => new TestCaseData(useAsync,
                          parallelism,
                          size).SetArgDisplayNames($"int[{size}], {parallelism}{(useAsync ? ", async" : "")}");

    foreach (var useAsync in Booleans)
    {
      yield return Case(useAsync,
                        0,
                        100);
      yield return Case(useAsync,
                        1,
                        10);
      yield return Case(useAsync,
                        2,
                        20);
      yield return Case(useAsync,
                        10,
                        100);
      yield return Case(useAsync,
                        100,
                        1000);
      yield return Case(useAsync,
                        -1,
                        10000);
    }
  }

  [Test]
  [TestCaseSource(nameof(ParallelSelectLimitCases))]
  public async Task ParallelSelectLimitShouldSucceed(bool useAsync,
                                                     int  parallelism,
                                                     int  n)
  {
    var counter    = 0;
    var maxCounter = 0;

    // We use a larger wait for infinite parallelism to ensure we can actually spawn thousands of tasks in parallel
    var identity = parallelism < 0
                     ? AsyncIdentity(200,
                                     400)
                     : AsyncIdentity(50,
                                     100);

    async Task<int> F(int x)
    {
      var count = Interlocked.Increment(ref counter);
      InterlockedMax(ref maxCounter,
                     count);

      await identity(x)
        .ConfigureAwait(false);
      Interlocked.Decrement(ref counter);
      return x;
    }

    var enumerable = useAsync
                       ? GenerateIntsAsync(n)
                         .ParallelSelect(new ParallelTaskOptions(parallelism),
                                         F)
                       : GenerateInts(n)
                         .ParallelSelect(new ParallelTaskOptions(parallelism),
                                         F);

    var x = await enumerable.ToListAsync()
                            .ConfigureAwait(false);
    var y = GenerateInts(n)
      .ToList();
    Assert.That(x,
                Is.EqualTo(y));

    switch (parallelism)
    {
      case > 0:
        Assert.That(maxCounter,
                    Is.EqualTo(parallelism));
        break;
      case 0:
        Assert.That(maxCounter,
                    Is.EqualTo(Environment.ProcessorCount));
        break;
      case < 0:
        Assert.That(maxCounter,
                    Is.EqualTo(n));
        break;
    }
  }

  [Test]
  [TestCase(false)]
  [TestCase(true)]
  public async Task UnorderedCompletionShouldSucceed(bool useAsync)
  {
    var firstDone = false;

    async Task<bool> F(int i)
    {
      if (i != 0)
      {
        return !firstDone;
      }

      await Task.Delay(100)
                .ConfigureAwait(false);
      firstDone = true;
      return true;
    }

    var enumerable = useAsync
                       ? GenerateIntsAsync(1000)
                         .ParallelSelect(new ParallelTaskOptions(2),
                                         F)
                       : GenerateInts(1000)
                         .ParallelSelect(new ParallelTaskOptions(2),
                                         F);
    var x = await enumerable.ToListAsync()
                            .ConfigureAwait(false);
    Assert.That(x,
                Is.All.True);
  }


  private static IEnumerable CancellationCases()
    =>
      from useAsync in Booleans
      from cancellationAware in Booleans
      from cancelLast in Booleans
      select new TestCaseData(useAsync,
                              cancellationAware,
                              cancelLast)
        .SetArgDisplayNames($"{(cancellationAware ? "aware" : "oblivious")}{(cancelLast ? ", last" : "")}{(useAsync ? ", async" : "")}");

  [Test]
  [TestCaseSource(nameof(CancellationCases))]
  public async Task CancellationShouldSucceed(bool useAsync,
                                              bool cancellationAware,
                                              bool cancelLast)
  {
    const int cancelAt = 100;

    var maxEntered = -1;
    var maxExited  = -1;
    var cts        = new CancellationTokenSource();

    async Task<int> F(int x)
    {
      if (x == cancelAt)
      {
        cts.Cancel();
      }

      InterlockedMax(ref maxEntered,
                     x);

      await Task.Delay(100,
                       cancellationAware
                         ? cts.Token
                         : CancellationToken.None)
                .ConfigureAwait(false);

      InterlockedMax(ref maxExited,
                     x);
      return x;
    }

    var enumerable = useAsync
                       ? GenerateInts(cancelLast
                                        ? cancelAt + 1
                                        : 100000)
                         .ParallelSelect(new ParallelTaskOptions(-1,
                                                                 cts.Token),
                                         F)
                       : GenerateIntsAsync(cancelLast
                                             ? cancelAt + 1
                                             : 100000,
                                           0,
                                           CancellationToken.None)
                         .ParallelSelect(new ParallelTaskOptions(-1,
                                                                 cts.Token),
                                         F);

    Assert.ThrowsAsync<TaskCanceledException>(async () =>
                                              {
                                                await foreach (var _ in enumerable.WithCancellation(CancellationToken.None))
                                                {
                                                }
                                              });

    await Task.Delay(200,
                     CancellationToken.None)
              .ConfigureAwait(false);

    Assert.That(maxEntered,
                Is.EqualTo(cancelAt));
    Assert.That(maxExited,
                Is.EqualTo(cancellationAware
                             ? -1
                             : cancelAt));
  }

  [Test]
  [TestCaseSource(nameof(CancellationCases))]
  public async Task ThrowingShouldSucceed(bool useAsync,
                                          bool cancellationAware,
                                          bool throwLast)
  {
    const int throwAt = 100;

    async Task<int> F(int x)
    {
      if (x == throwAt)
      {
        throw new ApplicationException();
      }

      await Task.Delay(100)
                .ConfigureAwait(false);
      return x;
    }

    var enumerable = useAsync
                       ? GenerateInts(throwLast
                                        ? throwAt + 1
                                        : 100000)
                         .ParallelSelect(new ParallelTaskOptions(-1),
                                         F)
                       : GenerateIntsAsync(throwLast
                                             ? throwAt + 1
                                             : 100000,
                                           0,
                                           CancellationToken.None)
                         .ParallelSelect(new ParallelTaskOptions(-1),
                                         F);

    await using var enumerator = enumerable.GetAsyncEnumerator();

    for (var i = 0; i < throwAt; ++i)
    {
      Assert.That(await enumerator.MoveNextAsync()
                                  .ConfigureAwait(false),
                  Is.EqualTo(true));
      Assert.That(enumerator.Current,
                  Is.EqualTo(i));
    }

    Assert.ThrowsAsync<ApplicationException>(async () => await enumerator.MoveNextAsync()
                                                                         .ConfigureAwait(false));
  }

  private static IEnumerable<int> GenerateInts(int n)
  {
    for (var i = 0; i < n; ++i)
    {
      yield return i;
    }
  }

  private static async IAsyncEnumerable<int> GenerateIntsAsync(int                                        n,
                                                               int                                        delay             = 0,
                                                               [EnumeratorCancellation] CancellationToken cancellationToken = default)
  {
    for (var i = 0; i < n; ++i)
    {
      if (delay > 0)
      {
        await Task.Delay(delay,
                         cancellationToken)
                  .ConfigureAwait(false);
      }
      else
      {
        await Task.Yield();
      }

      yield return i;
    }
  }

  private static Func<int, Task<int>> AsyncIdentity(int               delayMin          = 0,
                                                    int               delayMax          = 0,
                                                    CancellationToken cancellationToken = default)
    => async x =>
       {
         if (delayMax <= delayMin)
         {
           delayMax = delayMin + 1;
         }

         var rng = new Random(x);
         var delay = rng.Next(delayMin,
                              delayMax);
         if (delay > 0)
         {
           await Task.Delay(delay,
                            cancellationToken)
                     .ConfigureAwait(false);
         }
         else
         {
           await Task.Yield();
         }

         return x;
       };

  private static void InterlockedMax(ref int location,
                                     int     value)
  {
    // This is a typical instance of the CAS loop pattern:
    // https://learn.microsoft.com/en-us/dotnet/api/system.threading.interlocked.compareexchange#system-threading-interlocked-compareexchange(system-single@-system-single-system-single)

    // Red the current max at location
    var max = location;

    // repeat as long as current max in less than new value
    while (max < value)
    {
      // Tries to store the new value if max has not changed
      max = Interlocked.CompareExchange(ref location,
                                        value,
                                        max);
    }
  }
}
