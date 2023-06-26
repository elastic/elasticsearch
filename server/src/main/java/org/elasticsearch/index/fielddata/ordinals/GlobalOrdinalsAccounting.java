/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.index.fielddata.ordinals;

import org.elasticsearch.core.TimeValue;

/**
 * An interface global ordinals index field data instances can implement in order to keep track of building time costs.
 * There is no global ordinal field data interface, so this is its own thing now.
 * <p>
 * This is a little bit similar to {@link org.apache.lucene.util.Accountable}.
 * The building time is a big cost for global ordinals and one of its downsides.
 * Each time the an {@link org.apache.lucene.index.IndexReader} gets re-opened,
 * then global ordinals need to be rebuild. The cost depends on the cardinality of the field.
 */
public interface GlobalOrdinalsAccounting {

    /**
     * @return unique value count of global ordinals implementing this interface.
     */
    long getValueCount();

    /**
     * @return the total time spent building this global ordinal instance.
     */
    TimeValue getBuildingTime();

}
