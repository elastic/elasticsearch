/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.logical.local;

import org.elasticsearch.common.io.stream.NamedWriteable;
import org.elasticsearch.common.io.stream.Writeable;
import org.elasticsearch.compute.data.Block;
import org.elasticsearch.compute.data.Page;

import java.util.function.Supplier;

/**
 * Supplies fixed {@link Block}s for things calculated at plan time.
 * <p>
 *     This is {@link Writeable} so we can model {@code LOOKUP} and
 *     hash joins which have to go over the wire. But many implementers
 *     don't have to go over the wire and they should feel free to throw
 *     {@link UnsupportedOperationException}.
 * </p>
 */
public interface LocalSupplier extends Supplier<Page>, NamedWriteable {

    static LocalSupplier of(Page page) {
        return new ImmediateLocalSupplier(page);
    }

}
