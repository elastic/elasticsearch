/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

final class TopNFunction implements Cloneable, ToXContentObject, Comparable<TopNFunction> {
    private final String id;
    private int rank;
    private final String frameID;
    private final String fileID;
    private final int frameType;
    private final boolean inline;
    private final int addressOrLine;
    private final String functionName;
    private final int functionOffset;
    private final String sourceFilename;
    private final int sourceLine;
    private final String exeFilename;
    private long exclusiveCount;
    private long inclusiveCount;
    private double annualCO2TonsExclusive;
    private double annualCO2TonsInclusive;
    private double annualCostsUSDExclusive;
    private double annualCostsUSDInclusive;
    private final Map<String, Long> subGroups;

    TopNFunction(
        String id,
        String frameID,
        String fileID,
        int frameType,
        boolean inline,
        int addressOrLine,
        String functionName,
        int functionOffset,
        String sourceFilename,
        int sourceLine,
        String exeFilename
    ) {
        this(
            id,
            0,
            frameID,
            fileID,
            frameType,
            inline,
            addressOrLine,
            functionName,
            functionOffset,
            sourceFilename,
            sourceLine,
            exeFilename,
            0,
            0,
            0.0d,
            0.0d,
            0.0d,
            0.0d,
            new HashMap<>()
        );
    }

    TopNFunction(
        String id,
        int rank,
        String frameID,
        String fileID,
        int frameType,
        boolean inline,
        int addressOrLine,
        String functionName,
        int functionOffset,
        String sourceFilename,
        int sourceLine,
        String exeFilename,
        long exclusiveCount,
        long inclusiveCount,
        double annualCO2TonsExclusive,
        double annualCO2TonsInclusive,
        double annualCostsUSDExclusive,
        double annualCostsUSDInclusive,
        Map<String, Long> subGroups
    ) {
        this.id = id;
        this.rank = rank;
        this.frameID = frameID;
        this.fileID = fileID;
        this.frameType = frameType;
        this.inline = inline;
        this.addressOrLine = addressOrLine;
        this.functionName = functionName;
        this.functionOffset = functionOffset;
        this.sourceFilename = sourceFilename;
        this.sourceLine = sourceLine;
        this.exeFilename = exeFilename;
        this.exclusiveCount = exclusiveCount;
        this.inclusiveCount = inclusiveCount;
        this.annualCO2TonsExclusive = annualCO2TonsExclusive;
        this.annualCO2TonsInclusive = annualCO2TonsInclusive;
        this.annualCostsUSDExclusive = annualCostsUSDExclusive;
        this.annualCostsUSDInclusive = annualCostsUSDInclusive;
        this.subGroups = subGroups;
    }

    public String getId() {
        return this.id;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public long getExclusiveCount() {
        return exclusiveCount;
    }

    public void addExclusiveCount(long exclusiveCount) {
        this.exclusiveCount += exclusiveCount;
    }

    public long getInclusiveCount() {
        return inclusiveCount;
    }

    public void addInclusiveCount(long inclusiveCount) {
        this.inclusiveCount += inclusiveCount;
    }

    public void addAnnualCO2TonsExclusive(double annualCO2TonsExclusive) {
        this.annualCO2TonsExclusive += annualCO2TonsExclusive;
    }

    public void addAnnualCO2TonsInclusive(double annualCO2TonsInclusive) {
        this.annualCO2TonsInclusive += annualCO2TonsInclusive;
    }

    public void addAnnualCostsUSDExclusive(double annualCostsUSDExclusive) {
        this.annualCostsUSDExclusive += annualCostsUSDExclusive;
    }

    public void addAnnualCostsUSDInclusive(double annualCostsUSDInclusive) {
        this.annualCostsUSDInclusive += annualCostsUSDInclusive;
    }

    public void addSubGroups(Map<String, Long> subGroups) {
        for (Map.Entry<String, Long> subGroup : subGroups.entrySet()) {
            long count = this.subGroups.getOrDefault(subGroup.getKey(), 0L);
            this.subGroups.put(subGroup.getKey(), count + subGroup.getValue());
        }
    }

    @Override
    protected TopNFunction clone() {
        return new TopNFunction(
            id,
            rank,
            frameID,
            fileID,
            frameType,
            inline,
            addressOrLine,
            functionName,
            functionOffset,
            sourceFilename,
            sourceLine,
            exeFilename,
            exclusiveCount,
            inclusiveCount,
            annualCO2TonsExclusive,
            annualCO2TonsInclusive,
            annualCostsUSDExclusive,
            annualCostsUSDInclusive,
            new HashMap<>(subGroups)
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        // TODO: Clarify with Cauê which fields we *really* need
        builder.field("id", this.id);
        builder.field("rank", this.rank);
        builder.startObject("frame");
        builder.field("frame_id", this.frameID);
        builder.field("file_id", this.fileID);
        builder.field("frame_type", this.frameType);
        builder.field("inline", this.inline);
        builder.field("address_or_line", this.addressOrLine);
        builder.field("function_name", this.functionName);
        builder.field("function_offset", this.functionOffset);
        builder.field("file_name", this.sourceFilename);
        builder.field("line_number", this.sourceLine);
        builder.field("executable_file_name", this.exeFilename);
        builder.endObject();
        builder.field("sub_groups", subGroups);
        builder.field("count_exclusive", this.exclusiveCount);
        builder.field("count_inclusive", this.inclusiveCount);
        builder.field("annual_co2_tons_exclusive").rawValue(NumberUtils.doubleToString(annualCO2TonsExclusive));
        builder.field("annual_co2_tons_inclusive").rawValue(NumberUtils.doubleToString(annualCO2TonsInclusive));
        builder.field("annual_costs_usd_exclusive").rawValue(NumberUtils.doubleToString(annualCostsUSDExclusive));
        builder.field("annual_costs_usd_inclusive").rawValue(NumberUtils.doubleToString(annualCostsUSDInclusive));
        builder.endObject();
        return builder;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        TopNFunction that = (TopNFunction) o;
        return Objects.equals(id, that.id)
            && Objects.equals(rank, that.rank)
            && Objects.equals(frameID, that.frameID)
            && Objects.equals(fileID, that.fileID)
            && Objects.equals(frameType, that.frameType)
            && Objects.equals(inline, that.inline)
            && Objects.equals(addressOrLine, that.addressOrLine)
            && Objects.equals(functionName, that.functionName)
            && Objects.equals(functionOffset, that.functionOffset)
            && Objects.equals(sourceFilename, that.sourceFilename)
            && Objects.equals(sourceLine, that.sourceLine)
            && Objects.equals(exeFilename, that.exeFilename)
            && Objects.equals(exclusiveCount, that.exclusiveCount)
            && Objects.equals(inclusiveCount, that.inclusiveCount)
            && Objects.equals(annualCO2TonsExclusive, that.annualCO2TonsExclusive)
            && Objects.equals(annualCO2TonsInclusive, that.annualCO2TonsInclusive)
            && Objects.equals(annualCostsUSDExclusive, that.annualCostsUSDExclusive)
            && Objects.equals(annualCostsUSDInclusive, that.annualCostsUSDInclusive)
            && Objects.equals(subGroups, that.subGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            rank,
            frameID,
            fileID,
            frameType,
            inline,
            addressOrLine,
            functionName,
            functionOffset,
            sourceFilename,
            sourceLine,
            exeFilename,
            exclusiveCount,
            inclusiveCount,
            annualCO2TonsExclusive,
            annualCO2TonsInclusive,
            annualCostsUSDExclusive,
            annualCostsUSDInclusive,
            subGroups
        );
    }

    @Override
    public String toString() {
        return "TopNFunction{"
            + "id='"
            + id
            + '\''
            + ", rank="
            + rank
            + ", frameID='"
            + frameID
            + '\''
            + ", fileID='"
            + fileID
            + '\''
            + ", frameType="
            + frameType
            + ", inline="
            + inline
            + ", addressOrLine="
            + addressOrLine
            + ", functionName='"
            + functionName
            + '\''
            + ", functionOffset="
            + functionOffset
            + ", sourceFilename='"
            + sourceFilename
            + '\''
            + ", sourceLine="
            + sourceLine
            + ", exeFilename='"
            + exeFilename
            + '\''
            + ", exclusiveCount="
            + exclusiveCount
            + ", inclusiveCount="
            + inclusiveCount
            + ", annualCO2TonsExclusive="
            + annualCO2TonsExclusive
            + ", annualCO2TonsInclusive="
            + annualCO2TonsInclusive
            + ", annualCostsUSDExclusive="
            + annualCostsUSDExclusive
            + ", annualCostsUSDInclusive="
            + annualCostsUSDInclusive
            + ", subGroups="
            + subGroups
            + '}';
    }

    @Override
    public int compareTo(TopNFunction that) {
        if (this.exclusiveCount > that.exclusiveCount) {
            return 1;
        }
        if (this.exclusiveCount < that.exclusiveCount) {
            return -1;
        }
        return this.id.compareTo(that.id);
    }
}
