/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

public class InstanceTypeService {
    private final Map<InstanceType, CostEntry> costsPerDatacenter = new HashMap<>();

    public void load() {
        try (
            GZIPInputStream in = new GZIPInputStream(
                InstanceTypeService.class.getClassLoader().getResourceAsStream("profiling-costs.json.gz")
            )
        ) {
            XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in);
            if (parser.currentToken() == null) {
                parser.nextToken();
            }
            List<Map<String, Object>> rawData = XContentParserUtils.parseList(parser, XContentParser::map);
            for (Map<String, Object> entry : rawData) {
                costsPerDatacenter.put(InstanceType.fromCostSource(entry), CostEntry.fromSource(entry));
            }

        } catch (IOException e) {
            throw new UncheckedIOException(e);
        }
    }

    public CostEntry getCosts(InstanceType instance) {
        return costsPerDatacenter.get(instance);
    }
}
