/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;

import org.elasticsearch.xpack.sql.SqlIllegalArgumentException;
import org.elasticsearch.xpack.sql.analysis.catalog.EsType;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.NestedFieldAttribute;
import org.elasticsearch.xpack.sql.expression.RootFieldAttribute;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.NestedType;
import org.elasticsearch.xpack.sql.type.StringType;
import org.elasticsearch.xpack.sql.util.StringUtils;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import static org.elasticsearch.xpack.sql.util.CollectionUtils.combine;

public class CatalogTable extends LeafPlan {

    private final EsType type;
    private final List<Attribute> attrs;

    public CatalogTable(Location location, EsType type) {
        super(location);
        this.type = type;
        attrs = flatten(location, type.mapping()).collect(toList());
    }

    private static Stream<Attribute> flatten(Location location, Map<String, DataType> mapping) {
        return flatten(location, mapping, null, emptyList());
    }
    
    private static Stream<Attribute> flatten(Location location, Map<String, DataType> mapping, String parent, List<String> nestedParents) {
        return mapping.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .flatMap(e -> {
                    String name = parent != null ? parent + "." + e.getKey() : e.getKey();
                    DataType t = e.getValue();
                    if (t.isComplex() && !(t instanceof StringType)) {
                        if (t instanceof NestedType) {
                            return Stream.concat(Stream.of(new NestedFieldAttribute(location, name, t, nestedParents)), flatten(location, ((NestedType) t).properties(), name, combine(nestedParents, name)));
                        }
                        //                        if (t instanceof ObjectType) {
                        //                            return flatten(location, ((ObjectType) t).properties(), name, combine(nestedParents, name));
                        //                        }

                        throw new SqlIllegalArgumentException("Does not know how to handle complex type %s", t);
                    }
                    Attribute att = nestedParents.isEmpty() ? new RootFieldAttribute(location, name, t) : new NestedFieldAttribute(location, name, t, nestedParents);
                    return Stream.of(att);
                });
    }

    public EsType type() {
        return type;
    }

    @Override
    public List<Attribute> output() {
        return attrs;
    }

    @Override
    public boolean expressionsResolved() {
        return true;
    }

    @Override
    public int hashCode() {
        return Objects.hash(type);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        CatalogTable other = (CatalogTable) obj;
        return Objects.equals(type, other.type);
    }

    @Override
    public String nodeString() {
        return nodeName() + "[" + type + "]" + StringUtils.limitedToString(attrs);
    }
}