/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.expression.function.scalar;

import org.elasticsearch.common.io.stream.NamedWriteableRegistry;
import org.elasticsearch.xpack.esql.expression.function.grouping.GroupingWritables;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Case;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Greatest;
import org.elasticsearch.xpack.esql.expression.function.scalar.conditional.Least;
import org.elasticsearch.xpack.esql.expression.function.scalar.convert.FromAggregateMetricDouble;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateDiff;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateExtract;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateFormat;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateParse;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.DateTrunc;
import org.elasticsearch.xpack.esql.expression.function.scalar.date.Now;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.CIDRMatch;
import org.elasticsearch.xpack.esql.expression.function.scalar.ip.IpPrefix;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Atan2;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.E;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Hypot;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Log;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pi;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Pow;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Round;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.RoundTo;
import org.elasticsearch.xpack.esql.expression.function.scalar.math.Tau;
import org.elasticsearch.xpack.esql.expression.function.scalar.nulls.Coalesce;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.BitLength;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Concat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.EndsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Hash;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Left;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Locate;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Md5;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Repeat;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Replace;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Reverse;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Right;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha1;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Sha256;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Split;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.StartsWith;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.Substring;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToLower;
import org.elasticsearch.xpack.esql.expression.function.scalar.string.ToUpper;
import org.elasticsearch.xpack.esql.expression.predicate.logical.And;
import org.elasticsearch.xpack.esql.expression.predicate.logical.Or;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.In;
import org.elasticsearch.xpack.esql.expression.predicate.operator.comparison.InsensitiveEquals;

import java.util.ArrayList;
import java.util.List;

public class ScalarFunctionWritables {
    public static List<NamedWriteableRegistry.Entry> getNamedWriteables() {
        List<NamedWriteableRegistry.Entry> entries = new ArrayList<>();
        entries.add(And.ENTRY);
        entries.add(Atan2.ENTRY);
        entries.add(BitLength.ENTRY);
        entries.add(Case.ENTRY);
        entries.add(CIDRMatch.ENTRY);
        entries.add(Coalesce.ENTRY);
        entries.add(Concat.ENTRY);
        entries.add(E.ENTRY);
        entries.add(EndsWith.ENTRY);
        entries.add(FromAggregateMetricDouble.ENTRY);
        entries.add(Greatest.ENTRY);
        entries.add(Hash.ENTRY);
        entries.add(Hypot.ENTRY);
        entries.add(In.ENTRY);
        entries.add(InsensitiveEquals.ENTRY);
        entries.add(DateExtract.ENTRY);
        entries.add(DateDiff.ENTRY);
        entries.add(DateFormat.ENTRY);
        entries.add(DateParse.ENTRY);
        entries.add(DateTrunc.ENTRY);
        entries.add(IpPrefix.ENTRY);
        entries.add(Least.ENTRY);
        entries.add(Left.ENTRY);
        entries.add(Locate.ENTRY);
        entries.add(Log.ENTRY);
        entries.add(Md5.ENTRY);
        entries.add(Now.ENTRY);
        entries.add(Or.ENTRY);
        entries.add(Pi.ENTRY);
        entries.add(Pow.ENTRY);
        entries.add(Right.ENTRY);
        entries.add(Repeat.ENTRY);
        entries.add(Replace.ENTRY);
        entries.add(Reverse.ENTRY);
        entries.add(Round.ENTRY);
        entries.add(RoundTo.ENTRY);
        entries.add(Sha1.ENTRY);
        entries.add(Sha256.ENTRY);
        entries.add(Split.ENTRY);
        entries.add(Substring.ENTRY);
        entries.add(StartsWith.ENTRY);
        entries.add(Tau.ENTRY);
        entries.add(ToLower.ENTRY);
        entries.add(ToUpper.ENTRY);

        entries.addAll(GroupingWritables.getNamedWriteables());
        return entries;
    }
}
