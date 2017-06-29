/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.session;

import java.util.ArrayList;
import java.util.List;

import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.Schema;
import org.elasticsearch.xpack.sql.util.Assert;

import static java.util.Collections.singletonList;

import static java.util.Arrays.asList;

public abstract class Rows {

    public static Schema schema(List<Attribute> attr) {
        List<String> names = new ArrayList<>(attr.size());
        List<DataType> types = new ArrayList<>(attr.size());

        for (Attribute a : attr) {
            names.add(a.name());
            types.add(a.dataType());
        }
        return new Schema(names, types);
    }
    
    public static Schema schema(String n1, DataType t1) {
        return new Schema(singletonList(n1), singletonList(t1));
    }

    public static Schema schema(String n1, DataType t1, String n2, DataType t2) {
        return new Schema(asList(n1, n2), asList(t1, t2));
    }

    public static Schema schema(String n1, DataType t1, String n2, DataType t2, String n3, DataType t3) {
        return new Schema(asList(n1, n2, n3), asList(t1, t2, t3));
    }

    public static Schema schema(String n1, DataType t1, String n2, DataType t2, String n3, DataType t3, String n4, DataType t4) {
        return new Schema(asList(n1, n2, n3, n4), asList(t1, t2, t3, t4));
    }

    public static Schema schema(String n1, DataType t1, String n2, DataType t2, String n3, DataType t3, String n4, DataType t4, String n5, DataType t5) {
        return new Schema(asList(n1, n2, n3, n4, n5), asList(t1, t2, t3, t4, t5));
    }

    public static RowSetCursor of(List<Attribute> attrs, List<List<?>> values) {
        if (values.isEmpty()) {
            return empty(attrs);
        }
        
        if (values.size() == 1) {
            return singleton(attrs, values.get(0).toArray());
        }

        Schema schema = schema(attrs);
        return new ListRowSetCursor(schema, values);
    }

    public static RowSetCursor singleton(List<Attribute> attrs, Object... values) {
        Assert.isTrue(attrs.size() == values.length, "Schema %s and values %s are out of sync", attrs, values);
        return new SingletonRowSet(schema(attrs), values);
    }

    public static RowSetCursor empty(Schema schema) {
        return new EmptyRowSetCursor(schema);
    }

    public static RowSetCursor empty(List<Attribute> attrs) {
        return new EmptyRowSetCursor(schema(attrs));
    }
}
