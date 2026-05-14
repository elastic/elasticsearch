/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.index.fielddata;

import java.io.IOException;

/**
 * This interface enabled numeric values classes to be able to lead the iteration.
 * It is a clone of {@link org.apache.lucene.search.DocIdSetIterator}.
 */
public interface IterableNumericValues {

    /**
     *
     * @return -1 if the doc id has not be initialised by {@link #advance(int)} or another method,
     *         {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS} if the iterator has
     *         exhausted, otherwise the current docId
     */
    int docID();

    /**
     * Advances to the first beyond the current whose document number is greater than or equal to
     * the target docId, and returns the document number itself. Exhausts the iterator and returns {@link
     * org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS} if target is greater than the
     * highest document number in the set.
     *
     * The behavior of this method is undefined when the target is less than equal than {@link #docID}
     * or after the iterator has exhausted. Both cases may result in unpredicted behavior.
     *
     * @param target the target docId that needs to be greater than the {@link #docID}.
     * @return the docId greater or equal than the target with a value, if there is no other document
     *         then returns {@link org.apache.lucene.search.DocIdSetIterator#NO_MORE_DOCS}. If the target
     *         is not correct or the iterator is already exhausted, the behaviour is unpredictable.
     */
    int advance(int target) throws IOException;
}
