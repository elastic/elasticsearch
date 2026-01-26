/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.apache.iceberg.expressions.Expression;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.NodeUtils;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;
import org.elasticsearch.xpack.esql.datasources.s3.S3Configuration;
import org.elasticsearch.xpack.esql.plan.logical.IcebergRelation;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

/**
 * Physical plan node for reading from Iceberg tables or Parquet files.
 * This is created by LocalMapper from IcebergRelation logical plan node.
 * <p>
 * Extends {@link ExternalSourceExec} which provides common functionality for
 * external data source physical plan nodes.
 */
public class IcebergSourceExec extends ExternalSourceExec {

    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        PhysicalPlan.class,
        "IcebergSourceExec",
        IcebergSourceExec::readFrom
    );

    private final S3Configuration s3Config;
    private final String sourceType;
    private final Expression filter; // Iceberg filter expression for pushdown (nullable)

    public IcebergSourceExec(IcebergRelation relation) {
        this(
            relation.source(),
            relation.tablePath(),
            relation.metadata().s3Config(),
            relation.metadata().sourceType(),
            relation.output(),
            null, // No filter initially
            null  // Row size will be estimated by optimizer
        );
    }

    public IcebergSourceExec(
        Source source,
        String tablePath,
        S3Configuration s3Config,
        String sourceType,
        List<Attribute> attributes,
        Expression filter,
        Integer estimatedRowSize
    ) {
        super(source, tablePath, attributes, estimatedRowSize);
        this.s3Config = s3Config;
        this.sourceType = Objects.requireNonNull(sourceType, "sourceType must not be null");
        this.filter = filter; // Nullable - no filter means scan all rows
    }

    private static IcebergSourceExec readFrom(StreamInput in) throws IOException {
        var source = Source.readFrom((PlanStreamInput) in);
        String sourcePath = in.readString();
        String sourceType = in.readString();
        var attributes = in.readNamedWriteableCollectionAsList(Attribute.class);

        // Read S3 configuration (all fields nullable)
        String accessKey = in.readOptionalString();
        String secretKey = in.readOptionalString();
        String endpoint = in.readOptionalString();
        String region = in.readOptionalString();
        S3Configuration s3Config = S3Configuration.fromFields(accessKey, secretKey, endpoint, region);

        // Read filter (nullable) - Note: Iceberg Expression serialization would need
        // to be implemented for cross-node execution in distributed scenarios
        Expression filter = null;

        // Read estimated row size
        Integer estimatedRowSize = in.readOptionalVInt();

        return new IcebergSourceExec(source, sourcePath, s3Config, sourceType, attributes, filter, estimatedRowSize);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        Source.EMPTY.writeTo(out);
        out.writeString(sourcePath());
        out.writeString(sourceType);
        out.writeNamedWriteableCollection(output());

        // Write S3 configuration (all fields nullable, always write 4 fields)
        out.writeOptionalString(s3Config != null ? s3Config.accessKey() : null);
        out.writeOptionalString(s3Config != null ? s3Config.secretKey() : null);
        out.writeOptionalString(s3Config != null ? s3Config.endpoint() : null);
        out.writeOptionalString(s3Config != null ? s3Config.region() : null);

        // Write filter - Note: Iceberg Expression serialization would need
        // to be implemented for cross-node execution in distributed scenarios
        // TODO: Implement serialization if needed for cross-node execution

        // Write estimated row size
        out.writeOptionalVInt(estimatedRowSize());
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    /**
     * @return the path to the Iceberg table or Parquet file
     * @deprecated Use {@link #sourcePath()} instead
     */
    @Deprecated
    public String tablePath() {
        return sourcePath();
    }

    public S3Configuration s3Config() {
        return s3Config;
    }

    @Override
    public String sourceType() {
        return sourceType;
    }

    public Expression filter() {
        return filter;
    }

    /**
     * Create a new IcebergSourceExec with the given filter.
     * Used by PushFiltersToSource optimization rule.
     */
    public IcebergSourceExec withFilter(Expression newFilter) {
        return new IcebergSourceExec(source(), sourcePath(), s3Config, sourceType, output(), newFilter, estimatedRowSize());
    }

    @Override
    protected IcebergSourceExec withEstimatedRowSize(Integer newEstimatedRowSize) {
        return new IcebergSourceExec(source(), sourcePath(), s3Config, sourceType, output(), filter, newEstimatedRowSize);
    }

    @Override
    protected NodeInfo<? extends PhysicalPlan> info() {
        return NodeInfo.create(this, IcebergSourceExec::new, sourcePath(), s3Config, sourceType, output(), filter, estimatedRowSize());
    }

    @Override
    public int hashCode() {
        return Objects.hash(super.hashCode(), s3Config, sourceType, filter);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        if (!super.equals(obj)) {
            return false;
        }

        IcebergSourceExec other = (IcebergSourceExec) obj;
        return Objects.equals(s3Config, other.s3Config)
            && Objects.equals(sourceType, other.sourceType)
            && Objects.equals(filter, other.filter);
    }

    @Override
    public String nodeString(NodeStringFormat format) {
        String filterStr = filter != null ? "[filter=" + filter + "]" : "";
        return nodeName() + "[" + sourcePath() + "][" + sourceType + "]" + filterStr + NodeUtils.toString(output(), format);
    }
}
