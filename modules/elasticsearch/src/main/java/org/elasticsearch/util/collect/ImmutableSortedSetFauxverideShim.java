/*
 * Copyright (C) 2009 Google Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.elasticsearch.util.collect;

import org.elasticsearch.util.annotations.GwtCompatible;

/**
 * "Overrides" the {@link ImmutableSet} static methods that lack
 * {@link ImmutableSortedSet} equivalents with deprecated, exception-throwing
 * versions. This prevents accidents like the following:<pre>   {@code
 *
 *   List<Object> objects = ...;
 *   // Sort them:
 *   Set<Object> sorted = ImmutableSortedSet.copyOf(objects);
 *   // BAD CODE! The returned set is actually an unsorted ImmutableSet!}</pre>
 *
 * <p>While we could put the overrides in {@link ImmutableSortedSet} itself, it
 * seems clearer to separate these "do not call" methods from those intended for
 * normal use.
 *
 * @author Chris Povirk
 */
@GwtCompatible
abstract class ImmutableSortedSetFauxverideShim<E> extends ImmutableSet<E> {
  /**
   * Not supported. Use {@link ImmutableSortedSet#naturalOrder}, which offers
   * better type-safety, instead. This method exists only to hide
   * {@link ImmutableSet#builder} from consumers of {@code ImmutableSortedSet}.
   *
   * @throws UnsupportedOperationException always
   * @deprecated Use {@link ImmutableSortedSet#naturalOrder}, which offers
   *     better type-safety.
   */
  @Deprecated public static <E> ImmutableSortedSet.Builder<E> builder() {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain a
   * non-{@code Comparable} element.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass a parameter of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(Comparable)}.</b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(E element) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain a
   * non-{@code Comparable} element.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass the parameters of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(Comparable, Comparable)}.</b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(E e1, E e2) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain a
   * non-{@code Comparable} element.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass the parameters of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(Comparable, Comparable, Comparable)}.</b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(E e1, E e2, E e3) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain a
   * non-{@code Comparable} element.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass the parameters of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(Comparable, Comparable, Comparable, Comparable)}.
   * </b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(
      E e1, E e2, E e3, E e4) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain a
   * non-{@code Comparable} element.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass the parameters of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(
   *     Comparable, Comparable, Comparable, Comparable, Comparable)}. </b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(
      E e1, E e2, E e3, E e4, E e5) {
    throw new UnsupportedOperationException();
  }

  /**
   * Not supported. <b>You are attempting to create a set that may contain
   * non-{@code Comparable} elements.</b> Proper calls will resolve to the
   * version in {@code ImmutableSortedSet}, not this dummy version.
   *
   * @throws UnsupportedOperationException always
   * @deprecated <b>Pass parameters of type {@code Comparable} to use {@link
   *     ImmutableSortedSet#of(Comparable[])}.</b>
   */
  @Deprecated public static <E> ImmutableSortedSet<E> of(E... elements) {
    throw new UnsupportedOperationException();
  }

  /*
   * We would like to include an unsupported "<E> copyOf(Iterable<E>)" here,
   * providing only the properly typed
   * "<E extends Comparable<E>> copyOf(Iterable<E>)" in ImmutableSortedSet (and
   * likewise for the Iterator equivalent). However, due to a change in Sun's
   * interpretation of the JLS (as described at
   * http://bugs.sun.com/view_bug.do?bug_id=6182950), the OpenJDK 7 compiler
   * available as of this writing rejects our attempts. To maintain
   * compatibility with that version and with any other compilers that interpret
   * the JLS similarly, there is no definition of copyOf() here, and the
   * definition in ImmutableSortedSet matches that in ImmutableSet.
   * 
   * The result is that ImmutableSortedSet.copyOf() may be called on
   * non-Comparable elements. We have not discovered a better solution. In
   * retrospect, the static factory methods should have gone in a separate class
   * so that ImmutableSortedSet wouldn't "inherit" too-permissive factory
   * methods from ImmutableSet.
   */
}
