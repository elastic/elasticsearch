/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.ml.aggs.mapreduce;

import org.elasticsearch.search.DocValueFormat;

import java.util.ArrayList;
import java.util.List;

public final class FieldRegistry {

    public class Field {
        private final String name;
        private final int id;
        private final DocValueFormat format;

        private Field(String name, DocValueFormat format, int id) {
            this.name = name;
            this.format = format;
            this.id = id;
        }

        public String getName() {
            return name;
        }

        public int getId() {
            return id;
        }

        public DocValueFormat getFormat() {
            return format;
        }
    }

    List<Field> fields = new ArrayList<>();

    public FieldRegistry() {}

    public Field register(String name, DocValueFormat format) {
        Field field = new Field(name, format, fields.size());
        return field;
    }

    public Field getById(int id) {
        return fields.get(id);
    }

}
