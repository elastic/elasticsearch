/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.profiling.action;

import org.elasticsearch.xcontent.ToXContentObject;
import org.elasticsearch.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.Objects;

final class TopNFunction implements ToXContentObject, Comparable<TopNFunction> {
    private final String id;
    private int rank;
    private final int frameType;
    private final boolean inline;
    private final int addressOrLine;
    private final String functionName;
    private final String sourceFilename;
    private final int sourceLine;
    private final String exeFilename;
    private long selfCount;
    private long totalCount;
    private double selfAnnualCO2Tons;
    private double totalAnnualCO2Tons;
    private double selfAnnualCostsUSD;
    private double totalAnnualCostsUSD;
    private SubGroup subGroups;

    TopNFunction(
        String id,
        int frameType,
        boolean inline,
        int addressOrLine,
        String functionName,
        String sourceFilename,
        int sourceLine,
        String exeFilename
    ) {
        this(
            id,
            0,
            frameType,
            inline,
            addressOrLine,
            functionName,
            sourceFilename,
            sourceLine,
            exeFilename,
            0,
            0,
            0.0d,
            0.0d,
            0.0d,
            0.0d,
            null
        );
    }

    TopNFunction(
        String id,
        int rank,
        int frameType,
        boolean inline,
        int addressOrLine,
        String functionName,
        String sourceFilename,
        int sourceLine,
        String exeFilename,
        long selfCount,
        long totalCount,
        double selfAnnualCO2Tons,
        double totalAnnualCO2Tons,
        double selfAnnualCostsUSD,
        double totalAnnualCostsUSD,
        SubGroup subGroups
    ) {
        this.id = id;
        this.rank = rank;
        this.frameType = frameType;
        this.inline = inline;
        this.addressOrLine = addressOrLine;
        this.functionName = functionName;
        this.sourceFilename = sourceFilename;
        this.sourceLine = sourceLine;
        this.exeFilename = exeFilename;
        this.selfCount = selfCount;
        this.totalCount = totalCount;
        this.selfAnnualCO2Tons = selfAnnualCO2Tons;
        this.totalAnnualCO2Tons = totalAnnualCO2Tons;
        this.selfAnnualCostsUSD = selfAnnualCostsUSD;
        this.totalAnnualCostsUSD = totalAnnualCostsUSD;
        this.subGroups = subGroups;
    }

    public String getId() {
        return this.id;
    }

    public void setRank(int rank) {
        this.rank = rank;
    }

    public long getSelfCount() {
        return selfCount;
    }

    public void addSelfCount(long selfCount) {
        this.selfCount += selfCount;
    }

    public long getTotalCount() {
        return totalCount;
    }

    public void addTotalCount(long totalCount) {
        this.totalCount += totalCount;
    }

    public double getSelfAnnualCO2Tons() {
        return selfAnnualCO2Tons;
    }

    public void addSelfAnnualCO2Tons(double co2Tons) {
        this.selfAnnualCO2Tons += co2Tons;
    }

    public void addTotalAnnualCO2Tons(double co2Tons) {
        this.totalAnnualCO2Tons += co2Tons;
    }

    public double getSelfAnnualCostsUSD() {
        return selfAnnualCostsUSD;
    }

    public void addSelfAnnualCostsUSD(double costs) {
        this.selfAnnualCostsUSD += costs;
    }

    public void addTotalAnnualCostsUSD(double costs) {
        this.totalAnnualCostsUSD += costs;
    }

    public void addSubGroups(SubGroup subGroups) {
        if (this.subGroups == null) {
            this.subGroups = subGroups.copy();
        } else {
            this.subGroups.merge(subGroups);
        }
    }

    public TopNFunction copy() {
        return new TopNFunction(
            id,
            rank,
            frameType,
            inline,
            addressOrLine,
            functionName,
            sourceFilename,
            sourceLine,
            exeFilename,
            selfCount,
            totalCount,
            selfAnnualCO2Tons,
            totalAnnualCO2Tons,
            selfAnnualCostsUSD,
            totalAnnualCostsUSD,
            subGroups.copy()
        );
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("id", this.id);
        builder.field("rank", this.rank);
        builder.startObject("frame");
        builder.field("frame_type", this.frameType);
        builder.field("inline", this.inline);
        builder.field("address_or_line", this.addressOrLine);
        builder.field("function_name", this.functionName);
        builder.field("file_name", this.sourceFilename);
        builder.field("line_number", this.sourceLine);
        builder.field("executable_file_name", this.exeFilename);
        builder.endObject();
        if (subGroups != null) {
            builder.startObject("sub_groups");
            subGroups.toXContent(builder, params);
            builder.endObject();
        }
        builder.field("self_count", this.selfCount);
        builder.field("total_count", this.totalCount);
        builder.field("self_annual_co2_tons").rawValue(NumberUtils.doubleToString(selfAnnualCO2Tons));
        builder.field("total_annual_co2_tons").rawValue(NumberUtils.doubleToString(totalAnnualCO2Tons));
        builder.field("self_annual_costs_usd").rawValue(NumberUtils.doubleToString(selfAnnualCostsUSD));
        builder.field("total_annual_costs_usd").rawValue(NumberUtils.doubleToString(totalAnnualCostsUSD));
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
            && Objects.equals(frameType, that.frameType)
            && Objects.equals(inline, that.inline)
            && Objects.equals(addressOrLine, that.addressOrLine)
            && Objects.equals(functionName, that.functionName)
            && Objects.equals(sourceFilename, that.sourceFilename)
            && Objects.equals(sourceLine, that.sourceLine)
            && Objects.equals(exeFilename, that.exeFilename)
            && Objects.equals(selfCount, that.selfCount)
            && Objects.equals(totalCount, that.totalCount)
            && Objects.equals(selfAnnualCO2Tons, that.selfAnnualCO2Tons)
            && Objects.equals(totalAnnualCO2Tons, that.totalAnnualCO2Tons)
            && Objects.equals(selfAnnualCostsUSD, that.selfAnnualCostsUSD)
            && Objects.equals(totalAnnualCostsUSD, that.totalAnnualCostsUSD)
            && Objects.equals(subGroups, that.subGroups);
    }

    @Override
    public int hashCode() {
        return Objects.hash(
            id,
            rank,
            frameType,
            inline,
            addressOrLine,
            functionName,
            sourceFilename,
            sourceLine,
            exeFilename,
            selfCount,
            totalCount,
            selfAnnualCO2Tons,
            totalAnnualCO2Tons,
            selfAnnualCostsUSD,
            totalAnnualCostsUSD,
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
            + ", frameType="
            + frameType
            + ", inline="
            + inline
            + ", addressOrLine="
            + addressOrLine
            + ", functionName='"
            + functionName
            + '\''
            + ", sourceFilename='"
            + sourceFilename
            + '\''
            + ", sourceLine="
            + sourceLine
            + ", exeFilename='"
            + exeFilename
            + '\''
            + ", selfCount="
            + selfCount
            + ", totalCount="
            + totalCount
            + ", selfAnnualCO2Tons="
            + selfAnnualCO2Tons
            + ", totalAnnualCO2Tons="
            + totalAnnualCO2Tons
            + ", selfAnnualCostsUSD="
            + selfAnnualCostsUSD
            + ", totalAnnualCostsUSD="
            + totalAnnualCostsUSD
            + ", subGroups="
            + subGroups
            + '}';
    }

    @Override
    public int compareTo(TopNFunction that) {
        if (this.selfCount > that.selfCount) {
            return 1;
        }
        if (this.selfCount < that.selfCount) {
            return -1;
        }
        return this.id.compareTo(that.id);
    }
}
