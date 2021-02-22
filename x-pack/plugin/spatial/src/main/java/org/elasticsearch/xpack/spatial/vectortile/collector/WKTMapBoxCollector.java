/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.spatial.vectortile.collector;

import org.apache.lucene.index.LeafReaderContext;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.bytes.BytesArray;
import org.elasticsearch.index.fieldvisitor.SingleFieldsVisitor;
import org.elasticsearch.index.mapper.MappedFieldType;
import org.elasticsearch.search.lookup.SourceLookup;
import org.locationtech.jts.geom.Envelope;
import org.locationtech.jts.io.ParseException;
import org.locationtech.jts.io.WKTReader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * builds Vector Tile object from source documents. Geometries are expected to be
 * in WKT format.
 */
public class WKTMapBoxCollector extends AbstractMapBoxVectorTileCollector {

    final WKTReader wktReader = new WKTReader();
    final MappedFieldType sourceField;

    public WKTMapBoxCollector(MappedFieldType sourceField, Envelope tileEnvelope, String field) {
        super(tileEnvelope, field);
        this.sourceField = sourceField;
    }

    @Override
    public MapBoxVectorTileLeafCollector getVectorTileLeafCollector(LeafReaderContext context) {
        return docID -> {
            final List<Object> values = new ArrayList<>();
            final SingleFieldsVisitor visitor = new SingleFieldsVisitor(sourceField, values);
            context.reader().document(docID, visitor);
            final SourceLookup lookup = new SourceLookup();
            lookup.setSource(new BytesArray((BytesRef) values.get(0)));
            try {
                return wktReader.read((String) lookup.get(field));
            } catch (ParseException p) {
                throw new IOException(p);
            }
        };
    }
}
