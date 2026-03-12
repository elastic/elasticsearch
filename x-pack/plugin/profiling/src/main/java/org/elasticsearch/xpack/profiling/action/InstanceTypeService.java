/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.elasticsearch.common.xcontent.XContentParserUtils;
import org.elasticsearch.xcontent.XContentParser;
import org.elasticsearch.xcontent.XContentParserConfiguration;
import org.elasticsearch.xcontent.XContentType;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.zip.GZIPInputStream;

public final class InstanceTypeService {
    private InstanceTypeService() {}

    private static final Logger log = LogManager.getLogger(InstanceTypeService.class);

    private static final class Holder {
        private static final Map<InstanceType, CostEntry> COSTS_PER_DATACENTER;
        static {
            final StopWatch watch = new StopWatch("loadProfilingCostsData");
            final Map<InstanceType, CostEntry> tmp = new HashMap<>();
            final Map<Object, Object> objects = new HashMap<>();
            final Function<String, String> dedupString = s -> (String) objects.computeIfAbsent(s, Function.identity());

            // All files ar expected to exist.
            for (String provider : List.of("aws", "azure")) {
                String name = "profiling-costs-" + provider + ".json.gz";
                try (
                    GZIPInputStream in = new GZIPInputStream(InstanceTypeService.class.getClassLoader().getResourceAsStream(name));
                    XContentParser parser = XContentType.JSON.xContent().createParser(XContentParserConfiguration.EMPTY, in)
                ) {
                    if (parser.currentToken() == null) {
                        parser.nextToken();
                    }
                    List<Map<String, Object>> rawData = XContentParserUtils.parseList(parser, XContentParser::map);
                    for (Map<String, Object> entry : rawData) {
                        tmp.put(
                            new InstanceType(
                                provider,
                                dedupString.apply((String) entry.get("region")),
                                dedupString.apply((String) entry.get("instance_type"))
                            ),
                            (CostEntry) objects.computeIfAbsent(CostEntry.fromSource(entry), Function.identity())
                        );
                    }
                } catch (IOException e) {
                    throw new ExceptionInInitializerError(e);
                }
            }

            COSTS_PER_DATACENTER = Map.copyOf(tmp);

            log.debug(watch::report);
        }
    }

    public static CostEntry getCosts(InstanceType instance) {
        return Holder.COSTS_PER_DATACENTER.get(instance);
    }
}
