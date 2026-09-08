/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.gradle.internal.checkstyle;

import com.puppycrawl.tools.checkstyle.Checker;
import com.puppycrawl.tools.checkstyle.DefaultConfiguration;
import com.puppycrawl.tools.checkstyle.api.AuditEvent;
import com.puppycrawl.tools.checkstyle.api.AuditListener;
import com.puppycrawl.tools.checkstyle.api.CheckstyleException;

import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.List;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;

public class UnnecessaryFullyQualifiedClassLiteralCheckTests {

    @Test
    public void testImportedQualifiedClassLiteralIsRejected() throws CheckstyleException, IOException {
        assertViolations(
            """
                package fixture;
                import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
                class Example {
                    Class<?> type = org.elasticsearch.xpack.esql.plan.physical.TopNExec.class;
                    TopNExec value;
                }
                """,
            List.of(
                "Unnecessary fully qualified class literal 'org.elasticsearch.xpack.esql.plan.physical.TopNExec'; "
                    + "use imported type 'TopNExec'."
            )
        );
    }

    @Test
    public void testImportedSimpleClassLiteralIsAllowed() throws CheckstyleException, IOException {
        assertViolations(
            """
                package fixture;
                import org.elasticsearch.xpack.esql.plan.physical.TopNExec;
                class Example {
                    Class<?> type = TopNExec.class;
                    TopNExec value;
                }
                """,
            List.of()
        );
    }

    @Test
    public void testCollidingClassLiteralIsAllowed() throws CheckstyleException, IOException {
        assertViolations(
            """
                package fixture;
                import java.sql.Date;
                class Example {
                    Class<?> sqlType = Date.class;
                    Class<?> utilType = java.util.Date.class;
                }
                """,
            List.of()
        );
    }

    private static void assertViolations(String source, List<String> expectedViolations) throws CheckstyleException, IOException {
        File sourceFile = Files.createTempFile("checkstyle", ".java").toFile();
        try {
            Files.writeString(sourceFile.toPath(), source);
            List<String> violations = new ArrayList<>();
            Checker checker = new Checker();
            checker.setModuleClassLoader(UnnecessaryFullyQualifiedClassLiteralCheckTests.class.getClassLoader());
            checker.addListener(new CollectingAuditListener(violations));

            DefaultConfiguration treeWalker = new DefaultConfiguration("TreeWalker");
            DefaultConfiguration check = new DefaultConfiguration(UnnecessaryFullyQualifiedClassLiteralCheck.class.getName());
            check.addMessage(
                UnnecessaryFullyQualifiedClassLiteralCheck.MSG_KEY,
                "Unnecessary fully qualified class literal ''{0}''; use imported type ''{1}''."
            );
            treeWalker.addChild(check);
            DefaultConfiguration configuration = new DefaultConfiguration("Checker");
            configuration.addChild(treeWalker);
            checker.configure(configuration);
            checker.process(List.of(sourceFile));
            checker.destroy();

            assertThat(violations, equalTo(expectedViolations));
        } finally {
            Files.delete(sourceFile.toPath());
        }
    }

    private record CollectingAuditListener(List<String> violations) implements AuditListener {
        @Override
        public void auditStarted(AuditEvent event) {}

        @Override
        public void auditFinished(AuditEvent event) {}

        @Override
        public void fileStarted(AuditEvent event) {}

        @Override
        public void fileFinished(AuditEvent event) {}

        @Override
        public void addError(AuditEvent event) {
            violations.add(event.getMessage());
        }

        @Override
        public void addException(AuditEvent event, Throwable throwable) {
            throw new AssertionError(throwable);
        }
    }
}
