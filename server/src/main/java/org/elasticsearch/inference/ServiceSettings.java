/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.inference;

import org.elasticsearch.common.io.stream.VersionedNamedWriteable;
import org.elasticsearch.xcontent.ToXContentObject;

public interface ServiceSettings extends ToXContentObject, VersionedNamedWriteable {

    /**
     * Returns a {@link ToXContentObject} that only writes the exposed fields. Any hidden fields are not written.
     */
    ToXContentObject getFilteredXContentObject();

    /**
     * Similarity used in the service. Will be null if not applicable.
     *
     * @return similarity
     */
    default SimilarityMeasure similarity() {
        return null;
    }

    /**
     * Number of dimensions the service works with. Will be null if not applicable.
     *
     * @return number of dimensions
     */
    default Integer dimensions() {
        return null;
    }

}
