/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.plan.physical;

import org.elasticsearch.TransportVersion;
import org.elasticsearch.common.io.stream.BytesStreamOutput;
import org.elasticsearch.test.TransportVersionUtils;
import org.elasticsearch.xpack.esql.analysis.Analyzer;
import org.elasticsearch.xpack.esql.core.expression.Attribute;
import org.elasticsearch.xpack.esql.core.tree.Source;

import java.io.IOException;
import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class HashJoinExecSerializationTests extends AbstractPhysicalPlanSerializationTests<HashJoinExec> {
    public static HashJoinExec randomHashJoinExec(int depth) {
        Source source = randomSource();
        PhysicalPlan child = randomChild(depth);
        LocalSourceExec joinData = LocalSourceExecSerializationTests.randomLocalSourceExec();
        List<Attribute> leftFields = randomFields();
        List<Attribute> rightFields = randomFields();
        List<Attribute> output = randomFields();
        return new HashJoinExec(source, child, joinData, leftFields, rightFields, output, randomFrom(HashJoinExec.Mode.values()));
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
        HashJoinExec.Mode mode = instance.mode();
        switch (between(0, 5)) {
            case 0 -> child = randomValueOtherThan(child, () -> randomChild(0));
            case 1 -> joinData = randomValueOtherThan(joinData, LocalSourceExecSerializationTests::randomLocalSourceExec);
            case 2 -> leftFields = randomValueOtherThan(leftFields, HashJoinExecSerializationTests::randomFields);
            case 3 -> rightFields = randomValueOtherThan(rightFields, HashJoinExecSerializationTests::randomFields);
            case 4 -> output = randomValueOtherThan(output, HashJoinExecSerializationTests::randomFields);
            case 5 -> mode = randomValueOtherThan(mode, () -> randomFrom(HashJoinExec.Mode.values()));
            default -> throw new UnsupportedOperationException();
        }
        return new HashJoinExec(instance.source(), child, joinData, leftFields, rightFields, output, mode);
    }

    public void testSubqueryJoinModeRoundTripsAndSurvivesChildReplacement() throws IOException {
        HashJoinExec.Mode mode = randomFrom(HashJoinExec.Mode.LEFT_OUTER, HashJoinExec.Mode.INNER);
        HashJoinExec randomJoin = randomHashJoinExec(0);
        HashJoinExec join = new HashJoinExec(
            randomJoin.source(),
            randomJoin.left(),
            randomJoin.right(),
            randomJoin.leftFields(),
            randomJoin.rightFields(),
            List.copyOf(randomJoin.addedFields()),
            mode
        );
        assertThat(copyInstance(join, TransportVersion.current()).mode(), equalTo(mode));
        assertThat(join.replaceChildren(join.left(), join.right()).mode(), equalTo(mode));
    }

    public void testSubqueryJoinModeCannotBeSerializedToUnsupportedVersion() throws IOException {
        HashJoinExec randomJoin = randomHashJoinExec(0);
        HashJoinExec join = new HashJoinExec(
            randomJoin.source(),
            randomJoin.left(),
            randomJoin.right(),
            randomJoin.leftFields(),
            randomJoin.rightFields(),
            List.copyOf(randomJoin.addedFields()),
            randomFrom(HashJoinExec.Mode.LEFT_OUTER, HashJoinExec.Mode.INNER)
        );
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setTransportVersion(TransportVersionUtils.getPreviousVersion(Analyzer.ESQL_JOIN_SUBQUERY));
            IllegalStateException error = expectThrows(IllegalStateException.class, () -> join.writeTo(out));
            assertThat(error.getMessage(), containsString("without subquery join support"));
        }
    }

    @Override
    protected boolean alwaysEmptySource() {
        return true;
    }
}
