/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the "Elastic License
 * 2.0", the "GNU Affero General Public License v3.0 only", and the "Server Side
 * Public License v 1"; you may not use this file except in compliance with, at
 * your election, the "Elastic License 2.0", the "GNU Affero General Public
 * License v3.0 only", or the "Server Side Public License, v 1".
 */

package org.elasticsearch.entitlement.runtime.policy;

import org.elasticsearch.entitlement.runtime.api.NotEntitledException;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement;
import org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.FileData;
import org.elasticsearch.test.ESTestCase;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.NO_ENTITLEMENTS_MODULE;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.TEST_PATH_LOOKUP;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.createEmptyTestServerPolicy;
import static org.elasticsearch.entitlement.runtime.policy.PolicyManagerTests.makeClassInItsOwnModule;
import static org.elasticsearch.entitlement.runtime.policy.entitlements.FilesEntitlement.Mode.READ;
import static org.hamcrest.text.MatchesPattern.matchesPattern;

public class PolicyCheckerImplTests extends ESTestCase {
    public void testRequestingClassFastPath() throws IOException, ClassNotFoundException {
        var callerClass = makeClassInItsOwnModule();
        assertEquals(callerClass, checker(NO_ENTITLEMENTS_MODULE).requestingClass(callerClass));
    }

    public void testRequestingModuleWithStackWalk() throws IOException, ClassNotFoundException {
        var entitlementsClass = makeClassInItsOwnModule();    // A class in the entitlements library itself
        var instrumentedClass = makeClassInItsOwnModule();    // The class that called the check method
        var requestingClass = makeClassInItsOwnModule();      // This guy is always the right answer
        var ignorableClass = makeClassInItsOwnModule();

        var checker = checker(entitlementsClass.getModule());

        assertEquals(
            "Skip entitlement library and the instrumented method",
            requestingClass,
            checker.findRequestingFrame(
                Stream.of(entitlementsClass, instrumentedClass, requestingClass, ignorableClass).map(PolicyManagerTests.MockFrame::new)
            ).map(StackWalker.StackFrame::getDeclaringClass).orElse(null)
        );
        assertEquals(
            "Skip multiple library frames",
            requestingClass,
            checker.findRequestingFrame(
                Stream.of(entitlementsClass, entitlementsClass, instrumentedClass, requestingClass).map(PolicyManagerTests.MockFrame::new)
            ).map(StackWalker.StackFrame::getDeclaringClass).orElse(null)
        );
        assertThrows(
            "Non-modular caller frames are not supported",
            NullPointerException.class,
            () -> checker.findRequestingFrame(Stream.of(entitlementsClass, null).map(PolicyManagerTests.MockFrame::new))
        );
    }

    /**
     * Set up a situation where the file read entitlement check encounters
     * a strange {@link IOException} that is not {@link NoSuchFileException}.
     * Ensure that the resulting {@link NotEntitledException} makes
     * some effort to indicate the nature of the problem.
     */
    public void testIOExceptionFollowingSymlink() throws IOException {
        Path dir = createTempDir();
        Path symlink = dir.resolve("symlink");
        Path allegedDir = dir.resolve("not_a_dir");
        Path target = allegedDir.resolve("target");
        Files.createFile(allegedDir); // Not a dir!
        Files.createSymbolicLink(symlink, target);

        PathLookup testPathLookup = new PathLookup() {
            @Override
            public Path pidFile() {
                throw new UnsupportedOperationException();
            }

            @Override
            public Stream<Path> getBaseDirPaths(BaseDir baseDir) {
                return Stream.empty();
            }

            @Override
            public Stream<Path> resolveSettingPaths(BaseDir baseDir, String settingName) {
                throw new UnsupportedOperationException();
            }

            @Override
            public boolean isPathOnDefaultFilesystem(Path path) {
                // We need this to be on the default filesystem or it will be trivially allowed
                return true;
            }
        };

        var policyManager = new TestPolicyManager(
            createEmptyTestServerPolicy(),
            List.of(),
            Map.of(
                "testComponent",
                new Policy(
                    "testPolicy",
                    List.of(new Scope("testModule", List.of(new FilesEntitlement(List.of(FileData.ofPath(symlink, READ))))))
                )
            ),
            c -> PolicyManager.PolicyScope.plugin("testComponent", "testModule"),
            testPathLookup,
            List.of(),
            List.of()
        );
        policyManager.setActive(true);
        policyManager.setTriviallyAllowingTestCode(false);

        var checker = checker(NO_ENTITLEMENTS_MODULE, policyManager, testPathLookup);
        var exception = assertThrows(NotEntitledException.class, () -> checker.checkFileRead(getClass(), symlink, true));
        assertThat("The reason should be an exception of some sort", exception.getMessage(), matchesPattern(".*reason.*Exception.*"));
    }

    private static PolicyCheckerImpl checker(Module entitlementsModule) {
        // TODO: TEST_PATH_LOOKUP is always null at this point!
        return checker(entitlementsModule, null, TEST_PATH_LOOKUP);
    }

    private static PolicyCheckerImpl checker(Module entitlementsModule, PolicyManager policyManager, PathLookup testPathLookup) {
        return new PolicyCheckerImpl(Set.of(), entitlementsModule, policyManager, testPathLookup);
    }

}
