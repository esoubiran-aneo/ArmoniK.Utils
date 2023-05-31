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
using System.Linq;
using System.Threading;

using JetBrains.Annotations;

namespace ArmoniK.Utils;

public static class EnumerableExt
{
  /// <summary>
  ///   Convert an enumerable into a list, if it is not already a list
  ///   Beware that the return list may or may not be a reference to the input enumerable
  /// </summary>
  /// <param name="enumerable">The enumerable to convert into a list</param>
  /// <typeparam name="T">Type of the elements</typeparam>
  /// <returns>A list containing the same elements as the input enumerable</returns>
  [PublicAPI]
  public static IList<T> AsIList<T>(this IEnumerable<T>? enumerable)
  {
    if (enumerable is null)
    {
      return Array.Empty<T>();
    }

    return enumerable as IList<T> ?? enumerable.ToList();
  }

  /// <summary>
  ///   Convert an enumerable into a collection, if it is not already a collection
  ///   Beware that the return collection may or may not be a reference to the input enumerable
  /// </summary>
  /// <param name="enumerable">The enumerable to convert into a collection</param>
  /// <typeparam name="T">Type of the elements</typeparam>
  /// <returns>A collection containing the same elements as the input enumerable</returns>
  [PublicAPI]
  public static ICollection<T> AsICollection<T>(this IEnumerable<T>? enumerable)
  {
    if (enumerable is null)
    {
      return Array.Empty<T>();
    }

    return enumerable as ICollection<T> ?? enumerable.ToList();
  }

  /// <summary>
  ///   Split the elements of a sequence into chunks of size at most <paramref name="size" />.
  /// </summary>
  /// <remarks>
  ///   Every chunk except the last will be of size <paramref name="size" />.
  ///   The last chunk will contain the remaining elements and may be of a smaller size.
  /// </remarks>
  /// <param name="source">
  ///   An <see cref="IEnumerable{T}" /> whose elements to chunk.
  /// </param>
  /// <param name="size">
  ///   Maximum size of each chunk.
  /// </param>
  /// <typeparam name="TSource">
  ///   The type of the elements of source.
  /// </typeparam>
  /// <returns>
  ///   An <see cref="IEnumerable{T}" /> that contains the elements the input sequence split into chunks of size
  ///   <paramref name="size" />.
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">
  ///   <paramref name="size" /> is below 1.
  /// </exception>
  [PublicAPI]
  public static IEnumerable<TSource[]> ToChunks<TSource>(this IEnumerable<TSource>? source,
                                                         int                        size)
  {
    if (size < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(size));
    }

    if (source is null)
    {
      return Enumerable.Empty<TSource[]>();
    }

    return Chunk.Iterator(source,
                          size);
  }

  /// <summary>
  ///   Split the elements of a sequence into chunks of size at most <paramref name="size" />.
  /// </summary>
  /// <remarks>
  ///   Every chunk except the last will be of size <paramref name="size" />.
  ///   The last chunk will contain the remaining elements and may be of a smaller size.
  /// </remarks>
  /// <param name="source">
  ///   An <see cref="IEnumerable{T}" /> whose elements to chunk.
  /// </param>
  /// <param name="size">
  ///   Maximum size of each chunk.
  /// </param>
  /// <param name="maxDelay">
  ///   Maximum delay between the reading of a value and the yielding of the chunk containing this value.
  /// </param>
  /// <param name="cancellationToken">
  ///   Cancellation token used for stopping the enumeration.
  /// </param>
  /// <typeparam name="TSource">
  ///   The type of the elements of source.
  /// </typeparam>
  /// <returns>
  ///   An <see cref="IEnumerable{T}" /> that contains the elements the input sequence split into chunks of size
  ///   <paramref name="size" />.
  /// </returns>
  /// <exception cref="ArgumentOutOfRangeException">
  ///   <paramref name="size" /> is below 1.
  /// </exception>
  [PublicAPI]
  public static IAsyncEnumerable<TSource[]> ToChunksAsync<TSource>(this IAsyncEnumerable<TSource>? source,
                                                                   int                             size,
                                                                   TimeSpan                        maxDelay,
                                                                   CancellationToken               cancellationToken = default)
  {
    if (size < 1)
    {
      throw new ArgumentOutOfRangeException(nameof(size));
    }

    if (source is null)
    {
      return AsyncEnumerable.Empty<TSource[]>();
    }

    return Chunk.IteratorAsync(source,
                               size,
                               maxDelay,
                               cancellationToken);
  }
}
