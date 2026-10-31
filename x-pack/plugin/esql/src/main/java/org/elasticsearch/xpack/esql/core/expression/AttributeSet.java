/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.core.expression;

import org.elasticsearch.common.util.set.Sets;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Set;
import java.util.Spliterator;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

/**
 * Immutable set of attributes compared by {@link Attribute#semanticEquals} (their ids), in insertion order. See
 * {@link AttributeMap} for why attributes need this.
 * <p>
 * With {@link AttributeList#SHARED_OUTPUT_FEATURE_FLAG} enabled the set is stored as {@link AttributeSegments}, so a set of an
 * {@link AttributeList} shares the list's storage instead of copying it, and {@link #combine} shares this set's storage and
 * adds one segment for the new attributes. Otherwise it is stored as an {@link AttributeMap}.
 */
public class AttributeSet implements Set<Attribute> {

    private static final boolean SHARED = AttributeList.SHARED_OUTPUT_FEATURE_FLAG.isEnabled();

    public static final AttributeSet EMPTY = new AttributeSet(SHARED ? AttributeSegments.EMPTY : AttributeMap.emptyAttributeMap());

    /**
     * {@link AttributeSegments} when {@link #SHARED}, otherwise an {@code AttributeMap<Object>}. One field rather than two keeps
     * a set as small as before.
     */
    private final Object storage;

    private AttributeSet(Object storage) {
        this.storage = storage;
    }

    private AttributeSegments segments() {
        return (AttributeSegments) storage;
    }

    @SuppressWarnings("unchecked")
    private AttributeMap<Object> map() {
        return (AttributeMap<Object>) storage;
    }

    public static AttributeSet of(Attribute... attrs) {
        if (SHARED && attrs.length == 1) {
            return new AttributeSet(AttributeSegments.of(new Attribute[] { attrs[0] }));
        }
        return builder(attrs.length).addAll(Arrays.asList(attrs)).build();
    }

    /**
     * Builds a set of the given attributes. A set of an {@link AttributeList} shares the list's storage.
     */
    public static AttributeSet of(Collection<? extends Attribute> c) {
        if (SHARED && c instanceof AttributeList list) {
            return new AttributeSet(list.attributes());
        }
        return builder(c.size()).addAll(c).build();
    }

    public static <T> AttributeSet of(Collection<T> sources, Function<T, Collection<Attribute>> mapper) {
        if (sources.isEmpty()) {
            return AttributeSet.EMPTY;
        }
        var size = 0;
        var attributeSets = new ArrayList<Collection<Attribute>>(sources.size());
        for (T source : sources) {
            var attrs = mapper.apply(source);
            size += attrs.size();
            attributeSets.add(attrs);
        }
        var builder = AttributeSet.builder(size);
        for (Collection<Attribute> attributeSet : attributeSets) {
            builder.addAll(attributeSet);
        }
        return builder.build();
    }

    public static Builder builder() {
        return SHARED ? new Builder(0) : new Builder(AttributeMap.builder());
    }

    public static Builder builder(int expectedSize) {
        return SHARED ? new Builder(expectedSize) : new Builder(AttributeMap.builder(expectedSize));
    }

    /**
     * This set followed by the attributes of {@code other} that it doesn't contain. With {@link #SHARED} it shares this set's
     * storage.
     */
    public AttributeSet combine(AttributeSet other) {
        if (SHARED == false) {
            return new AttributeSet(map().combine(other.map()));
        }
        if (other.isEmpty()) {
            return this;
        }
        if (isEmpty()) {
            return other;
        }
        List<Attribute> added = new ArrayList<>();
        other.forEach(a -> {
            if (contains(a) == false) {
                added.add(a);
            }
        });
        return added.isEmpty() ? this : new AttributeSet(segments().append(added.toArray(new Attribute[0])));
    }

    public AttributeSet subtract(AttributeSet other) {
        if (SHARED == false) {
            return new AttributeSet(map().subtract(other.map()));
        }
        List<Attribute> kept = new ArrayList<>(size());
        forEach(a -> {
            if (other.contains(a) == false) {
                kept.add(a);
            }
        });
        return kept.size() == size() ? this : new AttributeSet(AttributeSegments.of(kept.toArray(new Attribute[0])));
    }

    public AttributeSet intersect(AttributeSet other) {
        if (SHARED == false) {
            return new AttributeSet(map().intersect(other.map()));
        }
        AttributeSet smaller = other.size() > size() ? this : other;
        AttributeSet larger = smaller == this ? other : this;
        List<Attribute> kept = new ArrayList<>(smaller.size());
        smaller.forEach(a -> {
            if (larger.contains(a)) {
                kept.add(a);
            }
        });
        return kept.size() == smaller.size() ? smaller : new AttributeSet(AttributeSegments.of(kept.toArray(new Attribute[0])));
    }

    public boolean subsetOf(AttributeSet other) {
        if (SHARED == false) {
            return map().subsetOf(other.map());
        }
        return size() <= other.size() && other.containsAll(this);
    }

    public Set<String> names() {
        if (SHARED == false) {
            return map().attributeNames();
        }
        Set<String> names = Sets.newLinkedHashSetWithExpectedSize(size());
        forEach(a -> names.add(a.name()));
        return names;
    }

    /**
     * Whether any attribute in this set is called {@code name}. Unlike {@code names().contains(name)} it doesn't build a set of
     * all the names.
     */
    public boolean containsName(String name) {
        if (SHARED) {
            return segments().containsName(name);
        }
        for (Attribute a : map().keySet()) {
            if (a.name().equals(name)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public void forEach(Consumer<? super Attribute> action) {
        if (SHARED) {
            segments().forEach(action);
        } else {
            map().forEach((k, v) -> action.accept(k));
        }
    }

    @Override
    public int size() {
        return SHARED ? segments().size() : map().size();
    }

    @Override
    public boolean isEmpty() {
        return SHARED ? segments().isEmpty() : map().isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        if (SHARED) {
            return o instanceof NamedExpression ne && segments().containsAttribute(ne.toAttribute());
        }
        return map().containsKey(o);
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        for (Object o : c) {
            if (contains(o) == false) {
                return false;
            }
        }
        return true;
    }

    @Override
    public Iterator<Attribute> iterator() {
        return SHARED ? segments().iterator() : map().keySet().iterator();
    }

    @Override
    public Object[] toArray() {
        return SHARED ? segments().toArray() : map().keySet().toArray();
    }

    @Override
    public <T> T[] toArray(T[] a) {
        return SHARED ? segments().toArray(a) : map().keySet().toArray(a);
    }

    @Override
    public boolean add(Attribute e) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean addAll(Collection<? extends Attribute> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException();
    }

    @Override
    public void clear() {
        throw new UnsupportedOperationException();
    }

    @Override
    public Spliterator<Attribute> spliterator() {
        return SHARED ? segments().spliterator() : map().keySet().spliterator();
    }

    @Override
    public boolean removeIf(Predicate<? super Attribute> filter) {
        throw new UnsupportedOperationException();
    }

    @Override
    public Stream<Attribute> stream() {
        return SHARED ? segments().stream() : map().keySet().stream();
    }

    @Override
    public Stream<Attribute> parallelStream() {
        return SHARED ? segments().parallelStream() : map().keySet().parallelStream();
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        AttributeSet other = (AttributeSet) obj;
        return SHARED ? size() == other.size() && containsAll(other) : map().equals(other.map());
    }

    /**
     * With {@link #SHARED}, the sum of the elements' {@link Attribute#semanticHash()}, which doesn't depend on order, like
     * {@link #equals}.
     */
    @Override
    public int hashCode() {
        if (SHARED == false) {
            return map().hashCode();
        }
        int hash = 0;
        for (Attribute a : segments()) {
            hash += a.semanticHash();
        }
        return hash;
    }

    @Override
    public String toString() {
        return SHARED ? segments().toString() : map().keySet().toString();
    }

    public Builder asBuilder() {
        return builder(size()).addAll(this);
    }

    /**
     * Collects attributes, ignoring one whose id is already present, and builds a set of them in the order they were first
     * added. Up to {@link AttributeSegments#LINEAR_SCAN_LIMIT} attributes are kept in an array and compared one by one, which
     * is cheaper than hashing for the many small sets of references the optimizer builds; past that it switches to an
     * {@link AttributeMap}. With {@link AttributeList#SHARED_OUTPUT_FEATURE_FLAG} disabled it always uses the map.
     */
    public static class Builder {

        // use the same name as in HashSet
        private static final Object PRESENT = new Object();

        private Attribute[] small;
        private int smallSize;
        private AttributeMap.Builder<Object> large;

        private Builder(AttributeMap.Builder<Object> large) {
            this.large = large;
        }

        private Builder(int expectedSize) {
            if (expectedSize > AttributeSegments.LINEAR_SCAN_LIMIT) {
                large = AttributeMap.builder(expectedSize);
            } else {
                small = new Attribute[AttributeSegments.LINEAR_SCAN_LIMIT];
            }
        }

        public Builder add(Attribute attr) {
            if (large != null) {
                large.put(attr, PRESENT);
            } else if (indexOfSmall(attr) < 0) {
                if (smallSize == small.length) {
                    switchToLarge();
                    large.put(attr, PRESENT);
                } else {
                    small[smallSize++] = attr;
                }
            }
            return this;
        }

        private void switchToLarge() {
            large = AttributeMap.builder(smallSize * 2);
            for (int i = 0; i < smallSize; i++) {
                large.put(small[i], PRESENT);
            }
            small = null;
            smallSize = 0;
        }

        private int indexOfSmall(Object o) {
            if (o instanceof NamedExpression ne) {
                Attribute attr = ne.toAttribute();
                for (int i = 0; i < smallSize; i++) {
                    if (small[i].semanticEquals(attr)) {
                        return i;
                    }
                }
            }
            return -1;
        }

        public boolean remove(Object o) {
            if (large != null) {
                return large.remove(o) != null;
            }
            int index = indexOfSmall(o);
            if (index < 0) {
                return false;
            }
            System.arraycopy(small, index + 1, small, index, smallSize - index - 1);
            small[--smallSize] = null;
            return true;
        }

        public Builder addAll(AttributeSet other) {
            if (SHARED == false) {
                large.putAll(other.map());
                return this;
            }
            other.forEach(this::add);
            return this;
        }

        public Builder addAll(Builder other) {
            if (SHARED == false) {
                large.putAll(other.large.build());
                return this;
            }
            if (other.large != null) {
                other.large.keySet().forEach(this::add);
            } else {
                for (int i = 0; i < other.smallSize; i++) {
                    add(other.small[i]);
                }
            }
            return this;
        }

        public Builder addAll(Collection<? extends Attribute> c) {
            for (var e : c) {
                add(e);
            }
            return this;
        }

        public boolean removeIf(Predicate<? super Attribute> filter) {
            if (large != null) {
                return large.keySet().removeIf(filter);
            }
            int kept = 0;
            for (int i = 0; i < smallSize; i++) {
                if (filter.test(small[i]) == false) {
                    small[kept++] = small[i];
                }
            }
            boolean removed = kept < smallSize;
            Arrays.fill(small, kept, smallSize, null);
            smallSize = kept;
            return removed;
        }

        public boolean contains(Object o) {
            return large != null ? large.containsKey(o) : indexOfSmall(o) >= 0;
        }

        public boolean isEmpty() {
            return large != null ? large.isEmpty() : smallSize == 0;
        }

        public AttributeSet build() {
            if (SHARED == false) {
                return new AttributeSet(large.build());
            }
            Attribute[] items;
            if (large != null) {
                items = large.keySet().toArray(new Attribute[0]);
            } else {
                items = Arrays.copyOf(small, smallSize);
            }
            return items.length == 0 ? EMPTY : new AttributeSet(AttributeSegments.of(items));
        }

        public void clear() {
            if (large != null) {
                large.keySet().clear();
            } else {
                Arrays.fill(small, 0, smallSize, null);
                smallSize = 0;
            }
        }
    }
}
