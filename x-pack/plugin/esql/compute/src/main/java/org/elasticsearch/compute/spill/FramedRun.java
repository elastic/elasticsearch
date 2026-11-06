/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.compute.spill;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.compute.spill.spi.SpillObject;

/**
 * A finished run: the object that holds it and what its header and footer say. Everything a reader needs to open it.
 */
public record FramedRun(String name, SpillObject object, FramedRunLayout.Header header, FramedRunLayout.Footer footer) {

    public long records() {
        return footer.records();
    }

    public int partitions() {
        return footer.partitions();
    }

    public long sizeInBytes() {
        return object.sizeInBytes();
    }

    public void delete(ActionListener<Void> listener) {
        object.delete(listener);
    }
}
