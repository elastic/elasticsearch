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
 * <dt>{@link org.elasticsearch.util.collect.Multimap}
 * <dd>A new type, which is similar to {@link java.util.Map}, but may contain
 *     multiple entries with the same key. Some behaviors of
 *     {@link org.elasticsearch.util.collect.Multimap} are left unspecified and are
 *     provided only by the subtypes mentioned below.
 *
 * <dt>{@link org.elasticsearch.util.collect.SetMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.collect.Multimap} which has
 *     order-independent equality and does not allow duplicate entries; that is,
 *     while a key may appear twice in a {@code SetMultimap}, each must map to a
 *     different value.  {@code SetMultimap} takes its name from the fact that
 *     the {@linkplain org.elasticsearch.util.collect.SetMultimap#get collection of
 *     values} associated with a given key fulfills the {@link java.util.Set}
 *     contract.
 *
 * <dt>{@link org.elasticsearch.util.collect.ListMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.collect.Multimap} which permits
 *     duplicate entries, supports random access of values for a particular key,
 *     and has <i>partially order-dependent equality</i> as defined by
 *     {@link org.elasticsearch.util.collect.ListMultimap#equals(Object)}. {@code
 *     ListMultimap} takes its name from the fact that the {@linkplain
 *     org.elasticsearch.util.collect.ListMultimap#get collection of values}
 *     associated with a given key fulfills the {@link java.util.List} contract.
 *
 * <dt>{@link org.elasticsearch.util.collect.SortedSetMultimap}
 * <dd>An extension of {@link org.elasticsearch.util.collect.SetMultimap} for which
 *     the {@linkplain org.elasticsearch.util.collect.SortedSetMultimap#get
 *     collection values} associated with a given key is a
 *     {@link java.util.SortedSet}.
 *
 * <dt>{@link org.elasticsearch.util.collect.Multiset}
 * <dd>An extension of {@link java.util.Collection} that may contain duplicate
 *     values like a {@link java.util.List}, yet has order-independent equality
 *     like a {@link java.util.Set}.  One typical use for a multiset is to
 *     represent a histogram.
 *
 * <dt>{@link org.elasticsearch.util.collect.BiMap}
 * <dd>An extension of {@link java.util.Map} that guarantees the uniqueness of
 *     its values as well as that of its keys. This is sometimes called an
 *     "invertible map," since the restriction on values enables it to support
 *     an {@linkplain org.elasticsearch.util.collect.BiMap#inverse inverse view} --
 *     which is another instance of {@code BiMap}.
 *
 * <dt>{@link org.elasticsearch.util.collect.ClassToInstanceMap}
 * <dd>An extension of {@link java.util.Map} that associates a raw type with an
 *     instance of that type.
 * </dl>
 *
 * <h2>Collection Implementations</h2>
 *
 * <h3>of {@link java.util.List}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableList}
 * </ul>
 *
 * <h3>of {@link java.util.Set}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableSet}
 * </ul>
 *
 * <h3>of {@link java.util.SortedSet}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableSortedSet}
 * </dl>
 *
 * <h3>of {@link java.util.Map}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableMap}
 * <dt>{@link org.elasticsearch.util.collect.MapMaker} (produced by)
 * </ul>
 *
 * <h3>of {@link java.util.SortedMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableSortedMap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.Multimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableMultimap}
 * <dt>{@link org.elasticsearch.util.collect.Multimaps#newMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.ListMultimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableListMultimap}
 * <dt>{@link org.elasticsearch.util.collect.ArrayListMultimap}
 * <dt>{@link org.elasticsearch.util.collect.LinkedListMultimap}
 * <dt>{@link org.elasticsearch.util.collect.Multimaps#newListMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.SetMultimap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableSetMultimap}
 * <dt>{@link org.elasticsearch.util.collect.HashMultimap}
 * <dt>{@link org.elasticsearch.util.collect.LinkedHashMultimap}
 * <dt>{@link org.elasticsearch.util.collect.TreeMultimap}
 * <dt>{@link org.elasticsearch.util.collect.Multimaps#newSetMultimap}
 * <dt>{@link org.elasticsearch.util.collect.Multimaps#newSortedSetMultimap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.Multiset}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableMultiset}
 * <dt>{@link org.elasticsearch.util.collect.ConcurrentHashMultiset}
 * <dt>{@link org.elasticsearch.util.collect.HashMultiset}
 * <dt>{@link org.elasticsearch.util.collect.LinkedHashMultiset}
 * <dt>{@link org.elasticsearch.util.collect.TreeMultiset}
 * <dt>{@link org.elasticsearch.util.collect.EnumMultiset}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.BiMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.HashBiMap}
 * <dt>{@link org.elasticsearch.util.collect.EnumBiMap}
 * <dt>{@link org.elasticsearch.util.collect.EnumHashBiMap}
 * </dl>
 *
 * <h3>of {@link org.elasticsearch.util.collect.ClassToInstanceMap}</h3>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ImmutableClassToInstanceMap}
 * <dt>{@link org.elasticsearch.util.collect.MutableClassToInstanceMap}
 * </dl>
 *
 * <h2>Skeletal implementations</h2>
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.AbstractIterator}
 * <dt>{@link org.elasticsearch.util.collect.UnmodifiableIterator}
 * </dl>
 *
 * <h2>Utilities</h2>
 *
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.Collections2}
 * <dt>{@link org.elasticsearch.util.collect.Iterators}
 * <dt>{@link org.elasticsearch.util.collect.Iterables}
 * <dt>{@link org.elasticsearch.util.collect.Lists}
 * <dt>{@link org.elasticsearch.util.collect.Maps}
 * <dt>{@link org.elasticsearch.util.collect.Ordering}
 * <dt>{@link org.elasticsearch.util.collect.Sets}
 * <dt>{@link org.elasticsearch.util.collect.Multisets}
 * <dt>{@link org.elasticsearch.util.collect.Multimaps}
 * <dt>{@link org.elasticsearch.util.collect.ObjectArrays}
 * </dl>

 * <h2>Forwarding collections</h2>
 *
 * <dl>
 * <dt>{@link org.elasticsearch.util.collect.ForwardingCollection }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingConcurrentMap }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingIterator }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingList }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingListIterator }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingMap }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingMapEntry }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingMultimap }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingMultiset }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingObject }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingQueue }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingSet }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingSortedMap }
 * <dt>{@link org.elasticsearch.util.collect.ForwardingSortedSet }
 * </dl>
 */
package org.elasticsearch.util.collect;
