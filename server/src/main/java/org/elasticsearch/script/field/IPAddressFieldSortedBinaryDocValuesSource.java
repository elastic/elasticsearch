/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.script.field;

import org.elasticsearch.index.fielddata.SortedBinaryDocValues;

public class IPAddressFieldSortedBinaryDocValuesSource extends IPAddressSortedBinaryDocValuesSource {

    protected IPAddressField field = null;

    public IPAddressFieldSortedBinaryDocValuesSource(SortedBinaryDocValues docValues) {
        super(docValues);
    }

    @Override
    public IPAddressField toScriptField(String name) {
        if (field == null) {
            field = new IPAddressField(name, supplier);
        }

        return field;
    }
}
