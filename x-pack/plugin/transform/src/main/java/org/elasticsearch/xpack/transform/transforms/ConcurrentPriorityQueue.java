/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.transform.transforms;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Function;

/**
 * {@link ConcurrentPriorityQueue} class provides a priority queue functionality with the additional capability of quickly finding a queue
 * element by the element's id.
 *
 * The implementation of this queue is thread-safe and utilizes locks.
 *
 * @param <E> Type of the element
 */
public class ConcurrentPriorityQueue<E> implements Iterable<E> {

    private static final Logger logger = LogManager.getLogger(ConcurrentPriorityQueue.class);

    private final Function<E, String> idExtractor;
    /** {@link SortedSet} allows us to quickly retrieve the element with the lowest priority. */
    private final SortedSet<E> elements;
    /** {@link Map} allows us to quickly retrieve the element by the element's id. */
    private final Map<String, E> elementsById;

    public ConcurrentPriorityQueue(Function<E, String> idExtractor, Function<E, Long> priorityExtractor) {
        this.idExtractor = Objects.requireNonNull(idExtractor);
        this.elements = new TreeSet<>(Comparator.comparing(Objects.requireNonNull(priorityExtractor)));
        this.elementsById = new HashMap<>();
    }

    /**
     * @return whether the queue is empty.
     */
    public synchronized boolean isEmpty() {
        return elements.isEmpty();
    }

    /**
     * @return the set of all the elements ids.
     */
    public synchronized Set<String> getElementIds() {
        return Collections.unmodifiableSet(new HashSet<>(elementsById.keySet()));
    }

    /**
     * @return the element with the *lowest* priority.
     */
    public synchronized E first() {
        return elements.first();
    }

    /**
     * Adds an element to the queue.
     *
     * @param element element to add
     * @return whether the element was added
     */
    public synchronized boolean add(E element) {
        String elementId = idExtractor.apply(element);
        logger.trace("add({}): {}", elementId, element);
        if (elementsById.containsKey(elementId)) {
            logger.trace("add({}) is a no-op as the element already exists", elementId);
            return false;
        }
        elementsById.put(elementId, element);
        elements.add(element);
        return true;
    }

    /**
     * Updates the element with the given id.
     *
     * @param elementId id of the element to update
     * @param transformer function used to modify the element. Must not modify the id
     */
    public synchronized void update(String elementId, Function<E, E> transformer) {
        E element = remove(elementId);
        if (element == null) {
            return;
        }
        E updatedElement = transformer.apply(element);
        if (elementId.equals(idExtractor.apply(updatedElement)) == false) {
            throw new IllegalStateException("Must not modify the element's id during update");
        }
        add(updatedElement);
    }

    /**
     * Removes the element with the given id from the queue.
     *
     * @param elementId id of the element to remove
     * @return the removed element
     */
    public synchronized E remove(String elementId) {
        logger.trace("remove({})", elementId);
        if (elementsById.containsKey(elementId) == false) {
            logger.trace("remove({}) is a no-op as the element does not exist", elementId);
            return null;
        }
        E element = elementsById.get(elementId);
        elements.remove(element);
        elementsById.remove(elementId);
        return element;
    }

    @Override
    public Iterator<E> iterator() {
        return elements.iterator();
    }
}
