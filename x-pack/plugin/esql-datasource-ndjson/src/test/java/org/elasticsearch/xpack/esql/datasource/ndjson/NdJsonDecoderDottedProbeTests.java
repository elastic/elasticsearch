/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.datasource.ndjson;

import org.elasticsearch.common.breaker.NoopCircuitBreaker;
import org.elasticsearch.common.util.BigArrays;
import org.elasticsearch.compute.data.BlockFactory;
import org.elasticsearch.compute.data.LongBlock;
import org.elasticsearch.compute.data.Page;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.expression.ReferenceAttribute;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.datasources.spi.ErrorPolicy;

import java.nio.charset.StandardCharsets;
import java.util.List;

/** PROBE (review-only): decoder behavior for a flat dotted key when the prefix is absent from the schema. */
public class NdJsonDecoderDottedProbeTests extends ESTestCase {

    public void testFlatKeyWithoutPrefixInSchema() throws Exception {
        BlockFactory blockFactory = BlockFactory.builder(BigArrays.NON_RECYCLING_INSTANCE).breaker(new NoopCircuitBreaker("none")).build();
        List<Attribute> schema = List.of(new ReferenceAttribute(Source.EMPTY, "languages.long", DataType.LONG));
        byte[] data = "{\"languages.long\":7}\n{\"languages.long\":8}\n".getBytes(StandardCharsets.UTF_8);
        NdJsonPageDecoder decoder = new NdJsonPageDecoder(
            data,
            0,
            data.length,
            null,
            schema,
            List.of("languages.long"),
            10,
            blockFactory,
            ErrorPolicy.STRICT,
            "probe",
            new NdJsonReaderCounters()
        );
        Page page = decoder.decodePage();
        LongBlock block = (LongBlock) page.getBlock(0);
        logger.info(
            "DECODER PROBE rows={} null0={} v0={} null1={} v1={}",
            page.getPositionCount(),
            block.isNull(0),
            block.isNull(0) ? -1 : block.getLong(0),
            page.getPositionCount() > 1 ? block.isNull(1) : null,
            page.getPositionCount() > 1 && block.isNull(1) == false ? block.getLong(1) : -1
        );
        page.releaseBlocks();
        decoder.close();
    }

    public void testInferrerFlatOnly() throws Exception {
        byte[] data = "{\"languages.long\":7}\n{\"languages.long\":8}\n".getBytes(StandardCharsets.UTF_8);
        List<Attribute> inferred = NdJsonSchemaInferrer.inferSchema(new java.io.ByteArrayInputStream(data), 100, null);
        logger.info("INFERRER PROBE names={}", inferred.stream().map(Attribute::name).toList());
    }
}
