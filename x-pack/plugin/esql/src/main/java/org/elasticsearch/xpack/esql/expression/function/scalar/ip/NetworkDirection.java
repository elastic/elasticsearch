/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar.ip;

import org.apache.lucene.document.InetAddressPoint;
import org.apache.lucene.util.BytesRef;
import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.network.NetworkDirectionUtils;
import org.elasticsearch.compute.ann.Evaluator;
import org.elasticsearch.compute.ann.Fixed;
import org.elasticsearch.compute.ann.Position;
import org.elasticsearch.compute.data.BytesRefBlock;
import org.elasticsearch.compute.operator.EvalOperator;
import org.elasticsearch.xpack.esql.core.expression.Expression;
import org.elasticsearch.xpack.esql.core.tree.NodeInfo;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.core.type.DataType;
import org.elasticsearch.xpack.esql.expression.function.Example;
import org.elasticsearch.xpack.esql.expression.function.FunctionInfo;
import org.elasticsearch.xpack.esql.expression.function.Param;
import org.elasticsearch.xpack.esql.expression.function.scalar.EsqlScalarFunction;
import org.elasticsearch.xpack.esql.io.stream.PlanStreamInput;

import java.io.IOException;
import java.net.InetAddress;
import java.util.Arrays;
import java.util.List;

import static org.elasticsearch.compute.ann.Fixed.Scope.THREAD_LOCAL;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.FIRST;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.SECOND;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.ParamOrdinal.THIRD;
import static org.elasticsearch.xpack.esql.core.expression.TypeResolutions.isIPAndExact;
import static org.elasticsearch.xpack.esql.expression.EsqlTypeResolutions.isStringAndExact;

/**
 * Returns the direction type (inbound, outbound, internal, external) given
 * a source IP address, destination IP address, and a list of internal networks.
 */
public class NetworkDirection extends EsqlScalarFunction {
    public static final NamedWriteableRegistry.Entry ENTRY = new NamedWriteableRegistry.Entry(
        Expression.class,
        "NetworkDirection",
        NetworkDirection::new
    );

    private final Expression sourceIpField;
    private final Expression destinationIpField;
    private final Expression internalNetworks;

    @FunctionInfo(
        returnType = "keyword",
        preview = true,
        description = "Returns the direction type (inbound, outbound, internal, external) given "
            + "a source IP address, destination IP address, and a list of internal networks.",
        examples = @Example(file = "ip", tag = "networkDirectionFromRowWithInlineNetworks")
    )
    public NetworkDirection(
        Source source,
        @Param(
            name = "source_ip",
            type = { "ip" },
            description = "Source IP address of type `ip` (both IPv4 and IPv6 are supported)."
        ) Expression sourceIpField,
        @Param(
            name = "destination_ip",
            type = { "ip" },
            description = "Destination IP address of type `ip` (both IPv4 and IPv6 are supported)."
        ) Expression destinationIpField,
        @Param(
            name = "internal_networks",
            type = { "keyword", "text" },
            description = "List of internal networks. Supports IPv4 and IPv6 addresses, ranges in CIDR notation, and named ranges."
        ) Expression internalNetworks
    ) {
        super(source, Arrays.asList(sourceIpField, destinationIpField, internalNetworks));
        this.sourceIpField = sourceIpField;
        this.destinationIpField = destinationIpField;
        this.internalNetworks = internalNetworks;
    }

    private NetworkDirection(StreamInput in) throws IOException {
        this(
            Source.readFrom((PlanStreamInput) in),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class),
            in.readNamedWriteable(Expression.class)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        source().writeTo(out);
        out.writeNamedWriteable(sourceIpField);
        out.writeNamedWriteable(destinationIpField);
        out.writeNamedWriteable(internalNetworks);
    }

    @Override
    public String getWriteableName() {
        return ENTRY.name;
    }

    @Override
    public Expression replaceChildren(List<Expression> newChildren) {
        return new NetworkDirection(source(), newChildren.get(0), newChildren.get(1), newChildren.get(2));
    }

    @Override
    protected NodeInfo<? extends Expression> info() {
        return NodeInfo.create(this, NetworkDirection::new, sourceIpField, destinationIpField, internalNetworks);
    }

    @Override
    public EvalOperator.ExpressionEvaluator.Factory toEvaluator(ToEvaluator toEvaluator) {
        var sourceIpEvaluatorSupplier = toEvaluator.apply(sourceIpField);
        var destinationIpEvaluatorSupplier = toEvaluator.apply(destinationIpField);
        var internalNetworksEvaluatorSupplier = toEvaluator.apply(internalNetworks);
        return new NetworkDirectionEvaluator.Factory(
            source(),
            context -> new BytesRef(16),
            context -> new BytesRef(),
            sourceIpEvaluatorSupplier,
            destinationIpEvaluatorSupplier,
            internalNetworksEvaluatorSupplier
        );
    }

    @Evaluator(warnExceptions = IllegalArgumentException.class)
    static void process(
        BytesRefBlock.Builder builder,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef scratch,
        @Fixed(includeInToString = false, scope = THREAD_LOCAL) BytesRef netScratch,
        BytesRef sourceIp,
        BytesRef destinationIp,
        @Position int position,
        BytesRefBlock networks
    ) {
        int valueCount = networks.getValueCount(position);
        if (valueCount == 0) {
            builder.appendNull();
            return;
        }
        int first = networks.getFirstValueIndex(position);

        System.arraycopy(sourceIp.bytes, sourceIp.offset, scratch.bytes, 0, sourceIp.length);
        InetAddress sourceIpAddress = InetAddressPoint.decode(scratch.bytes);
        System.arraycopy(destinationIp.bytes, destinationIp.offset, scratch.bytes, 0, destinationIp.length);
        InetAddress destinationIpAddress = InetAddressPoint.decode(scratch.bytes);

        boolean sourceInternal = false;
        boolean destinationInternal = false;

        for (int i = first; i < first + valueCount; i++) {
            if (NetworkDirectionUtils.inNetwork(sourceIpAddress, networks.getBytesRef(i, netScratch).utf8ToString())) {
                sourceInternal = true;
                break;
            }
        }
        for (int i = first; i < first + valueCount; i++) {
            if (NetworkDirectionUtils.inNetwork(destinationIpAddress, networks.getBytesRef(i, netScratch).utf8ToString())) {
                destinationInternal = true;
                break;
            }
        }

        builder.appendBytesRef(new BytesRef(NetworkDirectionUtils.getDirection(sourceInternal, destinationInternal)));
    }

    @Override
    public DataType dataType() {
        return DataType.KEYWORD;
    }

    @Override
    protected TypeResolution resolveType() {
        if (childrenResolved() == false) {
            return new TypeResolution("Unresolved children");
        }

        return isIPAndExact(sourceIpField, sourceText(), FIRST).and(isIPAndExact(destinationIpField, sourceText(), SECOND))
            .and(isStringAndExact(internalNetworks, sourceText(), THIRD));
    }

    public Expression sourceIpField() {
        return sourceIpField;
    }

    public Expression destinationIpField() {
        return destinationIpField;
    }

    public Expression internalNetworks() {
        return internalNetworks;
    }

    @Override
    public boolean foldable() {
        return sourceIpField.foldable() && destinationIpField.foldable() && internalNetworks.foldable();
    }
}
