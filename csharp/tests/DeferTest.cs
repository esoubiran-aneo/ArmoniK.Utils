﻿// This file is part of the ArmoniK project
//
// Copyright (C) ANEO, 2022-2022. All rights reserved.
//   W. Kirschenmann   <wkirschenmann@aneo.fr>
//   J. Gurhem         <jgurhem@aneo.fr>
//   D. Dubuc          <ddubuc@aneo.fr>
//   L. Ziane Khodja   <lzianekhodja@aneo.fr>
//   F. Lemaitre       <flemaitre@aneo.fr>
//   S. Djebbar        <sdjebbar@aneo.fr>
//   J. Fonseca        <jfonseca@aneo.fr>
//   D. Brasseur       <dbrasseur@aneo.fr>
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

using NUnit.Framework;

namespace ArmoniK.Utils.Tests;

public class DeferTest
{
  [Test]
  public void DeferEmptyShouldWork()
  {
    using var defer = Defer.Empty;
  }

  [Test]
  public void DeferShouldWork()
  {
    var i = 1;
    using (Defer.Create(() => i += 1))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(i,
                Is.EqualTo(2));
  }

  [Test]
  public void WrappedDeferShouldWork()
  {
    var i = 1;
    using (new DisposableWrapper(Defer.Create(() => i += 1)))
    {
      Assert.That(i,
                  Is.EqualTo(1));
    }

    Assert.That(i,
                Is.EqualTo(2));

    GC.Collect();
    GC.WaitForPendingFinalizers();

    Assert.That(i,
                Is.EqualTo(2));
  }

  private class DisposableWrapper : IDisposable
  {
    private readonly IDisposable disposable_;

    public DisposableWrapper(IDisposable disposable)
      => disposable_ = disposable;

    public void Dispose()
      => disposable_.Dispose();

    ~DisposableWrapper()
      => Dispose();
  }
}
