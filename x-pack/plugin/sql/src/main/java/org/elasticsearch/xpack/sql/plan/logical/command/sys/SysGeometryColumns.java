/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.xpack.sql.plan.logical.command.sys;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.common.Strings;
import org.elasticsearch.xpack.sql.analysis.index.EsIndex;
import org.elasticsearch.xpack.sql.expression.Attribute;
import org.elasticsearch.xpack.sql.expression.regex.LikePattern;
import org.elasticsearch.xpack.sql.plan.logical.command.Command;
import org.elasticsearch.xpack.sql.session.Rows;
import org.elasticsearch.xpack.sql.session.SchemaRowSet;
import org.elasticsearch.xpack.sql.session.SqlSession;
import org.elasticsearch.xpack.sql.tree.Location;
import org.elasticsearch.xpack.sql.tree.NodeInfo;
import org.elasticsearch.xpack.sql.type.DataType;
import org.elasticsearch.xpack.sql.type.EsField;

import java.sql.DatabaseMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.regex.Pattern;

import static java.util.Arrays.asList;
import static org.elasticsearch.xpack.sql.type.DataType.GEO_SHAPE;
import static org.elasticsearch.xpack.sql.type.DataType.INTEGER;
import static org.elasticsearch.xpack.sql.type.DataType.SHORT;

/**
 * System command designed to be used by JDBC / ODBC for column metadata, such as
 * {@link DatabaseMetaData#getColumns(String, String, String, String)}.
 */
public class SysGeometryColumns extends Command {

    private final String catalog;
    private final LikePattern indexPattern;
    private final LikePattern columnPattern;

    public SysGeometryColumns(Location location, String catalog, LikePattern indexPattern, LikePattern columnPattern) {
        super(location);
        this.catalog = catalog;
        this.indexPattern = indexPattern;
        this.columnPattern = columnPattern;
    }

    @Override
    protected NodeInfo<SysGeometryColumns> info() {
        return NodeInfo.create(this, SysGeometryColumns::new, catalog, indexPattern, columnPattern);
    }

    @Override
    public List<Attribute> output() {
        return asList(keyword("F_TABLE_CATALOG"),        // Cluster name
            keyword("F_TABLE_SCHEMA"),         // Always null - not supported
            keyword("F_TABLE_NAME"),           // Index name
            keyword("F_GEOMETRY_COLUMN"),      // Geo field name
            field("STORAGE_TYPE", INTEGER),    // Always 1 for now
            field("GEOMETRY_TYPE", INTEGER),   // 0 - for geoshapes or 1 - for geopoints
            field("COORD_DIMENSION", INTEGER), // Always 2
            field("SRID", INTEGER),            // Always 0 (the ID of the Spatial Reference System)
            keyword("TYPE")                    // GEOMETRY for geoshapes, POINT for geopoints
        );
    }

    @Override
    public void execute(SqlSession session, ActionListener<SchemaRowSet> listener) {
        String cluster = session.indexResolver().clusterName();

        // bail-out early if the catalog is present but differs
        if (Strings.hasText(catalog) && !cluster.equals(catalog)) {
            listener.onResponse(Rows.empty(output()));
            return;
        }

        String index = indexPattern != null ? indexPattern.asIndexNameWildcard() : "*";
        String regex = indexPattern != null ? indexPattern.asJavaRegex() : null;

        Pattern columnMatcher = columnPattern != null ? Pattern.compile(columnPattern.asJavaRegex()) : null;

        session.indexResolver().resolveAsSeparateMappings(index, regex, ActionListener.wrap(esIndices -> {
            List<List<?>> rows = new ArrayList<>();
            for (EsIndex esIndex : esIndices) {
                fillInRows(cluster, esIndex.name(), esIndex.mapping(), null, rows, columnMatcher);
            }

            listener.onResponse(Rows.of(output(), rows));
        }, listener::onFailure));
    }

    static void fillInRows(String clusterName, String indexName, Map<String, EsField> mapping, String prefix, List<List<?>> rows,
                           Pattern columnMatcher) {
        int pos = 0;
        for (Map.Entry<String, EsField> entry : mapping.entrySet()) {
            pos++; // JDBC is 1-based so we start with 1 here

            String name = entry.getKey();
            name = prefix != null ? prefix + "." + name : name;
            EsField field = entry.getValue();
            DataType type = field.getDataType();
            if ((columnMatcher == null || columnMatcher.matcher(name).matches()) && type == GEO_SHAPE) {
                rows.add(asList(clusterName,  // catalog
                    null,                 // schema is not supported
                    indexName,            // table name
                    name,                 // column name
                    1,                    // storage type
                    0,                    // geometry type (only geoshapes are supported at the moment)
                    2,                    // dimension
                    0,                    // srid,
                    "GEOMETRY"            // type name
                ));
            }
            if (field.getProperties() != null) {
                fillInRows(clusterName, indexName, field.getProperties(), name, rows, columnMatcher);
            }
        }
    }

    @Override
    public int hashCode() {
        return Objects.hash(catalog, indexPattern, columnPattern);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        SysGeometryColumns other = (SysGeometryColumns) obj;
        return Objects.equals(catalog, other.catalog)
            && Objects.equals(indexPattern, other.indexPattern)
            && Objects.equals(columnPattern, other.columnPattern);
    }
}
