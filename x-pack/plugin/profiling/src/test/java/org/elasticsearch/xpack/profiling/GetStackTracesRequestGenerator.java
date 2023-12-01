/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.index.query.QueryBuilder;
import org.junit.Assert;

import java.util.Iterator;
import java.util.List;

class GetStackTracesRequestGenerator implements Iterable<GetStackTracesRequest> {
    private final List<Integer> sampleSizes;
    private int sampleSizesPos = 0;

    private final List<Double> requestedDurations;
    private int requestedDurationsPos = 0;
    private final List<Double> awsCostFactors;
    private int awsCostFactorsPos = 0;
    private final List<QueryBuilder> querys;
    private int querysPos = 0;
    private final List<String> indices;
    private int indicesPos = 0;
    private final List<String> stackTraceIds;
    private int stackTraceIdsPos = 0;
    private final List<Double> customCO2PerKWHs;
    private int customCO2PerKWHsPos = 0;
    private final List<Double> datacenterPUEs;
    private int datacenterPUEsPos = 0;
    private final List<Double> perCoreWattX86s;
    private int perCoreWattX86sPos = 0;
    private final List<Double> perCoreWattARM64s;
    private int perCoreWattARM64sPos = 0;
    private final List<Double> customCostPerCoreHours;
    private int customCostPerCoreHoursPos = 0;
    private GetStackTracesRequest curRequest;
    private boolean done = false;

    GetStackTracesRequestGenerator(
        List<Integer> sampleSizes,
        List<Double> requestedDurations,
        List<Double> awsCostFactors,
        List<QueryBuilder> querys,
        List<String> indices,
        List<String> stackTraceIds,
        List<Double> customCO2PerKWHs,
        List<Double> datacenterPUEs,
        List<Double> perCoreWattX86s,
        List<Double> perCoreWattARM64s,
        List<Double> customCostPerCoreHours
    ) {
        this.sampleSizes = sampleSizes;
        this.requestedDurations = requestedDurations;
        this.awsCostFactors = awsCostFactors;
        this.querys = querys;
        this.indices = indices;
        this.stackTraceIds = stackTraceIds;
        this.customCO2PerKWHs = customCO2PerKWHs;
        this.datacenterPUEs = datacenterPUEs;
        this.perCoreWattX86s = perCoreWattX86s;
        this.perCoreWattARM64s = perCoreWattARM64s;
        this.customCostPerCoreHours = customCostPerCoreHours;
    }

    public GetStackTracesRequest createRequest() {
        return new GetStackTracesRequest(
            sampleSizes.get(sampleSizesPos),
            requestedDurations.get(requestedDurationsPos),
            awsCostFactors.get(awsCostFactorsPos),
            querys.get(querysPos),
            indices.get(indicesPos),
            stackTraceIds.get(stackTraceIdsPos),
            customCO2PerKWHs.get(customCO2PerKWHsPos),
            datacenterPUEs.get(datacenterPUEsPos),
            perCoreWattX86s.get(perCoreWattX86sPos),
            perCoreWattARM64s.get(perCoreWattARM64sPos),
            customCostPerCoreHours.get(customCostPerCoreHoursPos)
        );
    }

    public void check(GetStackTracesRequest request) {
        Assert.assertEquals(curRequest, request);
    }

    @Override
    public Iterator<GetStackTracesRequest> iterator() {
        return new Iterator<>() {
            @Override
            public boolean hasNext() {
                return done == false;
            }

            @Override
            public GetStackTracesRequest next() {
                curRequest = createRequest();

                sampleSizesPos++;
                if (sampleSizesPos >= sampleSizes.size()) {
                    sampleSizesPos = 0;
                    requestedDurationsPos++;
                    if (requestedDurationsPos >= requestedDurations.size()) {
                        requestedDurationsPos = 0;
                        awsCostFactorsPos++;
                        if (awsCostFactorsPos >= awsCostFactors.size()) {
                            awsCostFactorsPos = 0;
                            querysPos++;
                            if (querysPos >= querys.size()) {
                                querysPos = 0;
                                indicesPos++;
                                if (indicesPos >= indices.size()) {
                                    indicesPos = 0;
                                    stackTraceIdsPos++;
                                    if (stackTraceIdsPos >= stackTraceIds.size()) {
                                        stackTraceIdsPos = 0;
                                        customCO2PerKWHsPos++;
                                        if (customCO2PerKWHsPos >= customCO2PerKWHs.size()) {
                                            customCO2PerKWHsPos = 0;
                                            datacenterPUEsPos++;
                                            if (datacenterPUEsPos >= datacenterPUEs.size()) {
                                                datacenterPUEsPos = 0;
                                                perCoreWattX86sPos++;
                                                if (perCoreWattX86sPos >= perCoreWattX86s.size()) {
                                                    perCoreWattX86sPos = 0;
                                                    perCoreWattARM64sPos++;
                                                    if (perCoreWattARM64sPos >= perCoreWattARM64s.size()) {
                                                        perCoreWattARM64sPos = 0;
                                                        customCostPerCoreHoursPos++;
                                                        if (customCostPerCoreHoursPos >= customCostPerCoreHours.size()) {
                                                            customCostPerCoreHoursPos = 0;
                                                            done = true;
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }

                return curRequest;
            }
        };
    }
}
