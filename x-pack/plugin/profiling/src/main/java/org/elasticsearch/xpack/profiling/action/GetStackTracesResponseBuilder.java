/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import java.time.Instant;
import java.util.Map;

class GetStackTracesResponseBuilder {
    private Map<String, StackTrace> stackTraces;
    private Instant start;
    private Instant end;
    private int totalFrames;
    private Map<String, StackFrame> stackFrames;
    private Map<String, String> executables;
    private Map<TraceEventID, TraceEvent> stackTraceEvents;
    private double samplingRate;
    private long totalSamples;
    private Double requestedDuration;
    private final Double awsCostFactor;
    private final Double azureCostFactor;
    private final Double customCO2PerKWH;
    private final Double customDatacenterPUE;
    private final Double customPerCoreWattX86;
    private final Double customPerCoreWattARM64;
    private final Double customCostPerCoreHour;

    public void setStackTraces(Map<String, StackTrace> stackTraces) {
        this.stackTraces = stackTraces;
    }

    public Instant getStart() {
        return start;
    }

    public void setStart(Instant start) {
        this.start = start;
    }

    public Instant getEnd() {
        return end;
    }

    public void setEnd(Instant end) {
        this.end = end;
    }

    public void setTotalFrames(int totalFrames) {
        this.totalFrames = totalFrames;
    }

    public void addTotalFrames(int numFrames) {
        this.totalFrames += numFrames;
    }

    public void setStackFrames(Map<String, StackFrame> stackFrames) {
        this.stackFrames = stackFrames;
    }

    public void setExecutables(Map<String, String> executables) {
        this.executables = executables;
    }

    public void setStackTraceEvents(Map<TraceEventID, TraceEvent> stackTraceEvents) {
        this.stackTraceEvents = stackTraceEvents;
    }

    public Map<TraceEventID, TraceEvent> getStackTraceEvents() {
        return stackTraceEvents;
    }

    public void setSamplingRate(double rate) {
        this.samplingRate = rate;
    }

    public double getSamplingRate() {
        return samplingRate;
    }

    public void setRequestedDuration(Double requestedDuration) {
        this.requestedDuration = requestedDuration;
    }

    public double getRequestedDuration() {
        if (requestedDuration != null) {
            return requestedDuration;
        }
        // If "requested_duration" wasn't specified, we use the time range from the query response.
        return end.getEpochSecond() - start.getEpochSecond();
    }

    public Double getAWSCostFactor() {
        return awsCostFactor;
    }

    public Double getAzureCostFactor() {
        return azureCostFactor;
    }

    public Double getCustomCO2PerKWH() {
        return customCO2PerKWH;
    }

    public Double getCustomDatacenterPUE() {
        return customDatacenterPUE;
    }

    public Double getCustomPerCoreWattX86() {
        return customPerCoreWattX86;
    }

    public Double getCustomPerCoreWattARM64() {
        return customPerCoreWattARM64;
    }

    public Double getCustomCostPerCoreHour() {
        return customCostPerCoreHour;
    }

    public void setTotalSamples(long totalSamples) {
        this.totalSamples = totalSamples;
    }

    GetStackTracesResponseBuilder(GetStackTracesRequest request) {
        this.requestedDuration = request.getRequestedDuration();
        this.awsCostFactor = request.getAwsCostFactor();
        this.azureCostFactor = request.getAzureCostFactor();
        this.customCO2PerKWH = request.getCustomCO2PerKWH();
        this.customDatacenterPUE = request.getCustomDatacenterPUE();
        this.customPerCoreWattX86 = request.getCustomPerCoreWattX86();
        this.customPerCoreWattARM64 = request.getCustomPerCoreWattARM64();
        this.customCostPerCoreHour = request.getCustomCostPerCoreHour();
    }

    public GetStackTracesResponse build() {
        // Merge the TraceEvent data into the StackTraces.
        if (stackTraces != null) {
            for (Map.Entry<TraceEventID, TraceEvent> entry : stackTraceEvents.entrySet()) {
                TraceEventID traceEventID = entry.getKey();
                StackTrace stackTrace = stackTraces.get(traceEventID.stacktraceID());
                if (stackTrace != null) {
                    TraceEvent event = entry.getValue();
                    if (event.subGroups != null) {
                        stackTrace.subGroups = event.subGroups;
                    }
                    stackTrace.count += event.count;
                    stackTrace.annualCO2Tons += event.annualCO2Tons;
                    stackTrace.annualCostsUSD += event.annualCostsUSD;
                }
            }
        }
        return new GetStackTracesResponse(stackTraces, stackFrames, executables, stackTraceEvents, totalFrames, samplingRate, totalSamples);
    }
}
