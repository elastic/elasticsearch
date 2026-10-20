/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;

public class HashJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<HashJoinExec> {
    public static HashJoinExec randomHashJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        LocalSourceExec joinData = LocalSourceExecSerializationTests.randomLocalSourceExec();
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> output = randomFields();
        return new HashJoinExec(source, child, joinData, leftFields, rightFields, output, randomBoolean());
    }

    private static List<Attribute> randomFields() {
        return randomFieldAttributes(1, 5, false);
    }

    @Override
    protected HashJoinExec createTestInstance() {
        return randomHashJoinExec(0);
    }

    @Override
    protected HashJoinExec mutateInstance(HashJoinExec instance) throws IOException {
        PhysicalPlan child = instance.left();
        PhysicalPlan joinData = instance.joinData();
        List<Attribute> leftFields = randomFieldAttributes(1, 5, false);
        List<Attribute> rightFields = randomFieldAttributes(1, 5, false);
        List<Attribute> output = randomFieldAttributes(1, 5, false);
        boolean repeatedKeys = instance.repeatedKeys();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> joinData = randomValueOtherThan(joinData, LocalSourceExecSerializationTests::randomLocalSourceExec);
            case 2 -> leftFields = randomValueOtherThan(leftFields, HashJoinExecSerializationTests::randomFields);
            case 3 -> rightFields = randomValueOtherThan(rightFields, HashJoinExecSerializationTests::randomFields);
            case 4 -> output = randomValueOtherThan(output, HashJoinExecSerializationTests::randomFields);
            case 5 -> repeatedKeys = repeatedKeys == false;
            default -> throw new UnsupportedOperationException();
        }
        return new HashJoinExec(instance.source(), child, joinData, leftFields, rightFields, output, repeatedKeys);
    }

    public void testRepeatedKeyModeRoundTripsAndSurvivesChildReplacement() throws IOException {
        HashJoinExec randomJoin = randomHashJoinExec(0);
        HashJoinExec join = new HashJoinExec(
            randomJoin.source(),
            randomJoin.left(),
            randomJoin.right(),
            randomJoin.leftFields(),
            randomJoin.rightFields(),
            List.copyOf(randomJoin.addedFields()),
            true
        );
        assertTrue(copyInstance(join, TransportVersion.current()).repeatedKeys());
        assertTrue(join.replaceChildren(join.left(), join.right()).repeatedKeys());
    }

    public void testRepeatedKeyModeCannotBeSerializedToUnsupportedVersion() throws IOException {
        HashJoinExec randomJoin = randomHashJoinExec(0);
        HashJoinExec join = new HashJoinExec(
            randomJoin.source(),
            randomJoin.left(),
            randomJoin.right(),
            randomJoin.leftFields(),
            randomJoin.rightFields(),
            List.copyOf(randomJoin.addedFields()),
            true
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersion.fromId(Analyzer.ESQL_JOIN_SUBQUERY.id() - 1));
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> join.writeTo(out));
            assertThat(error.getMessage(), containsString("without subquery join support"));
        }
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
