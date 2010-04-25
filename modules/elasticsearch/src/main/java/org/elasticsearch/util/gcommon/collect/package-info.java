/*
 * Copyright (C) 2007 Google Inc.
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

/**
 * This package contains generic collection interfaces and implementations, and
 * other utilities for working with collections.
 *
 * <h2>Collection Types</h2>
 *
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimap}
 * <dd>A new type, which is similar to {@link java.util.Map}, but may contain
 *     multiple entries with the same key. Some behaviors of
 *     {@link org.elasticsearch.util.gcommon.collect.Multimap} are left unspecified and are
 *     provided only by the subtypes mentioned below.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.SetMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.gcommon.collect.Multimap} which has
 *     order-independent equality and does not allow duplicate entries; that is,
 *     while a key may appear twice in a {@code SetMultimap}, each must map to a
 *     different value.  {@code SetMultimap} takes its name from the fact that
 *     the {@linkplain org.elasticsearch.util.gcommon.collect.SetMultimap#get collection of
 *     values} associated with a given key fulfills the {@link java.util.Set}
 *     contract.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ListMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.gcommon.collect.Multimap} which permits
 *     duplicate entries, supports random access of values for a particular key,
 *     and has <i>partially order-dependent equality</i> as defined by
 *     {@link org.elasticsearch.util.gcommon.collect.ListMultimap#equals(Object)}. {@code
 *     ListMultimap} takes its name from the fact that the {@linkplain
 *     org.elasticsearch.util.gcommon.collect.ListMultimap#get collection of values}
 *     associated with a given key fulfills the {@link java.util.List} contract.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.SortedSetMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.gcommon.collect.SetMultimap} for which
 *     the {@linkplain org.elasticsearch.util.gcommon.collect.SortedSetMultimap#get
 *     collection values} associated with a given key is a
 *     {@link java.util.SortedSet}.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multiset}
 * <dd>An extension of {@link java.util.Collection} that may contain duplicate
 *     values like a {@link java.util.List}, yet has order-independent equality
 *     like a {@link java.util.Set}.  One typical use for a multiset is to
 *     represent a histogram.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.BiMap}
 * <dd>An extension of {@link java.util.Map} that guarantees the uniqueness of
 *     its values as well as that of its keys. This is sometimes called an
 *     "invertible map," since the restriction on values enables it to support
 *     an {@linkplain org.elasticsearch.util.gcommon.collect.BiMap#inverse inverse view} --
 *     which is another instance of {@code BiMap}.
 *
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ClassToInstanceMap}
 * <dd>An extension of {@link java.util.Map} that associates a raw type with an
 *     instance of that type.
 * </dl>
 *
 * <h2>Collection Implementations</h2>
 *
 * <h3>of {@link java.util.List}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableList}
 * </ul>
 *
 * <h3>of {@link java.util.Set}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableSet}
 * </ul>
 *
 * <h3>of {@link java.util.SortedSet}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableSortedSet}
 * </dl>
 *
 * <h3>of {@link java.util.Map}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableMap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.MapMaker} (produced by)
 * </ul>
 *
 * <h3>of {@link java.util.SortedMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableSortedMap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.Multimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimaps#newMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.ListMultimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableListMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ArrayListMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.LinkedListMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimaps#newListMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.SetMultimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableSetMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.HashMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.LinkedHashMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.TreeMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimaps#newSetMultimap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimaps#newSortedSetMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.Multiset}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableMultiset}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ConcurrentHashMultiset}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.HashMultiset}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.LinkedHashMultiset}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.TreeMultiset}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.EnumMultiset}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.BiMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.HashBiMap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.EnumBiMap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.EnumHashBiMap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.gcommon.collect.ClassToInstanceMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ImmutableClassToInstanceMap}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.MutableClassToInstanceMap}
 * </dl>
 *
 * <h2>Skeletal implementations</h2>
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.AbstractIterator}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.UnmodifiableIterator}
 * </dl>
 *
 * <h2>Utilities</h2>
 *
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Collections2}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Iterators}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Iterables}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Lists}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Maps}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Ordering}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Sets}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multisets}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.Multimaps}
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ObjectArrays}
 * </dl>

 * <h2>Forwarding collections</h2>
 *
 * <dl>
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingCollection }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingConcurrentMap }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingIterator }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingList }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingListIterator }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingMap }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingMapEntry }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingMultimap }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingMultiset }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingObject }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingQueue }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingSet }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingSortedMap }
 * <dt>{@link org.elasticsearch.util.gcommon.collect.ForwardingSortedSet }
 * </dl>
 */
package org.elasticsearch.util.gcommon.collect;
