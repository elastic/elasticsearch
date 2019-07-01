package org.elasticsearch.gradle;

import org.gradle.api.tasks.Input;
import org.gradle.api.tasks.Nested;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.ListIterator;
import java.util.function.Supplier;
import java.util.stream.Collectors;

public class LazyPropertyList<T> extends AbstractLazyPropertyCollection implements List<T> {

    private final List<PropertyListEntry<T>> delegate = new ArrayList<>();

    public LazyPropertyList(String name) {
        super(name);
    }

    public LazyPropertyList(String name, Object owner) {
        super(name, owner);
    }

    @Override
    public int size() {
        return delegate.size();
    }

    @Override
    public boolean isEmpty() {
        return delegate.isEmpty();
    }

    @Override
    public boolean contains(Object o) {
        return delegate.stream().anyMatch(entry -> entry.getValue().equals(o));
    }

    @Override
    public Iterator<T> iterator() {
        return delegate.stream().peek(this::validate).map(PropertyListEntry::getValue).iterator();
    }

    @Override
    public Object[] toArray() {
        return delegate.stream().peek(this::validate).map(PropertyListEntry::getValue).toArray();
    }

    @Override
    public <T1> T1[] toArray(T1[] a) {
        return delegate.stream().peek(this::validate).map(PropertyListEntry::getValue).collect(Collectors.toList()).toArray(a);
    }

    @Override
    public boolean add(T t) {
        return delegate.add(new PropertyListEntry<>(() -> t, PropertyNormalization.DEFAULT));
    }

    public boolean add(Supplier<T> supplier) {
        return delegate.add(new PropertyListEntry<>(supplier, PropertyNormalization.DEFAULT));
    }

    public boolean add(Supplier<T> supplier, PropertyNormalization normalization) {
        return delegate.add(new PropertyListEntry<>(supplier, normalization));
    }

    @Override
    public boolean remove(Object o) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support remove()");
    }

    @Override
    public boolean containsAll(Collection<?> c) {
        return delegate.stream().map(PropertyListEntry::getValue).collect(Collectors.toList()).containsAll(c);
    }

    @Override
    public boolean addAll(Collection<? extends T> c) {
        c.forEach(this::add);
        return true;
    }

    @Override
    public boolean addAll(int index, Collection<? extends T> c) {
        int i = index;
        for (T item : c) {
            this.add(i++, item);
        }
        return true;
    }

    @Override
    public boolean removeAll(Collection<?> c) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support removeAll()");
    }

    @Override
    public boolean retainAll(Collection<?> c) {
        throw new UnsupportedOperationException(this.getClass().getName() + " does not support retainAll()");
    }

    @Override
    public void clear() {
        delegate.clear();
    }

    @Override
    public T get(int index) {
        PropertyListEntry<T> entry = delegate.get(index);
        validate(entry);
        return entry.getValue();
    }

    @Override
    public T set(int index, T element) {
        return delegate.set(index, new PropertyListEntry<>(() -> element, PropertyNormalization.DEFAULT)).getValue();
    }

    @Override
    public void add(int index, T element) {
        delegate.add(index, new PropertyListEntry<>(() -> element, PropertyNormalization.DEFAULT));
    }

    @Override
    public T remove(int index) {
        return delegate.remove(index).getValue();
    }

    @Override
    public int indexOf(Object o) {
        for (int i = 0; i < delegate.size(); i++) {
            if (delegate.get(i).getValue().equals(o)) {
                return i;
            }
        }

        return -1;
    }

    @Override
    public int lastIndexOf(Object o) {
        int lastIndex = -1;
        for (int i = 0; i < delegate.size(); i++) {
            if (delegate.get(i).getValue().equals(o)) {
                lastIndex = i;
            }
        }

        return lastIndex;
    }

    @Override
    public ListIterator<T> listIterator() {
        return delegate.stream().map(PropertyListEntry::getValue).collect(Collectors.toList()).listIterator();
    }

    @Override
    public ListIterator<T> listIterator(int index) {
        return delegate.stream().peek(this::validate).map(PropertyListEntry::getValue).collect(Collectors.toList()).listIterator(index);
    }

    @Override
    public List<T> subList(int fromIndex, int toIndex) {
        return delegate.stream()
            .peek(this::validate)
            .map(PropertyListEntry::getValue)
            .collect(Collectors.toList())
            .subList(fromIndex, toIndex);
    }

    @Override
    @Nested
    List<? extends Object> getNormalizedCollection() {
        return delegate.stream()
            .peek(this::validate)
            .filter(entry -> entry.getNormalization() != PropertyNormalization.IGNORE_VALUE)
            .collect(Collectors.toList());
    }

    private void validate(PropertyListEntry<T> entry) {
        assertNotNull(entry.getValue(), "entry");
    }

    private class PropertyListEntry<T> {
        private final Supplier<T> supplier;
        private final PropertyNormalization normalization;

        PropertyListEntry(Supplier<T> supplier, PropertyNormalization normalization) {
            this.supplier = supplier;
            this.normalization = normalization;
        }

        public PropertyNormalization getNormalization() {
            return normalization;
        }

        @Input
        public T getValue() {
            assertNotNull(supplier, "supplier");
            return supplier.get();
        }
    }
}
