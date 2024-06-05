/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata;

import java.io.IOException;

public interface FormattedDocValues {
    /**
     * Advance the doc values reader to the provided doc.
     *
     * @return false if there are no values for this document, true otherwise
     */
    boolean advanceExact(int docId) throws IOException;

    /**
     * A count of the number of values at this document.
     */
    int docValueCount() throws IOException;

    /**
     * Load and format the next value.
     */
    Object nextValue() throws IOException;
}
