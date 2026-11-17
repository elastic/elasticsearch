/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */

package org.elasticsearch.xpack.esql.sourcefunction;

import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.metadata.ProjectMetadata;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.xpack.esql.core.expression.NamedExpression;
import org.elasticsearch.xpack.esql.core.tree.Source;
import org.elasticsearch.xpack.esql.parser.ParsingException;
import org.elasticsearch.xpack.esql.plan.logical.LeafPlan;

import java.util.List;

import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;

public class SourceFunctionRegistryTests extends ESTestCase {
    /** A function that is only a name: the registry resolves one, it never calls through to it. */
    private record NamedOnly(String name) implements SourceFunction {

        @Override
        public SourceFunctionSignature signature() {
            throw new AssertionError("the registry must not read a call's shape");
        }

        @Override
        public void validate(SourceFunctionInvocation call) {
            throw new AssertionError("the registry must not validate a call");
        }

        @Override
        public void toRelation(
            SourceFunctionInvocation call,
            List<NamedExpression> metadataFields,
            ProjectMetadata projectMetadata,
            ActionListener<LeafPlan> listener
        ) {
            throw new AssertionError("the registry must not bind a call");
        }
    }

    public void testResolveIgnoresCase() {
        SourceFunction function = new NamedOnly("thing");
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(function));
        for (String written : List.of("thing", "THING", "Thing", "tHiNg")) {
            assertSame(written, function, registry.resolve(Source.EMPTY, written));
        }
    }

    public void testUnknownNameListsTheRegisteredOnes() {
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(new NamedOnly("beta"), new NamedOnly("alpha")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "gamma"));
        assertThat(e.getMessage(), containsString("unknown source function [gamma], expected one of [alpha, beta]"));
    }

    /**
     * A misspelling is answered with the name that was meant, as an unknown scalar function is. The full listing is
     * the fallback rather than the answer, because a user who typed a name close to a real one is not browsing.
     */
    public void testUnknownNameOffersTheCloseOne() {
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(new NamedOnly("letters"), new NamedOnly("numbers")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "letrers"));
        assertThat(e.getMessage(), containsString("unknown source function [letrers], did you mean [letters]?"));
    }

    /**
     * Case is not a misspelling: the comparison lowercases the written name, since the keys are lowercase by
     * registration and a character-by-character distance would otherwise find nothing close to a shouted name.
     */
    public void testACloseNameIsFoundWhateverTheCase() {
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(new NamedOnly("letters")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "LETRERS"));
        assertThat(e.getMessage(), containsString("unknown source function [LETRERS], did you mean [letters]?"));
    }

    /** Several close names are all offered, nearest first, rather than one being picked for the user. */
    public void testUnknownNameOffersEveryCloseOne() {
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(new NamedOnly("letters"), new NamedOnly("letter")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "letterz"));
        assertThat(e.getMessage(), containsString("unknown source function [letterz], did you mean any of ["));
        assertThat(e.getMessage(), containsString("letters"));
        assertThat(e.getMessage(), containsString("letter"));
    }

    /** And a name close to nothing registered still gets the listing, so neither arm swallows the other. */
    public void testAnUnrelatedNameStillGetsTheListing() {
        SourceFunctionRegistry registry = new SourceFunctionRegistry(List.of(new NamedOnly("letters"), new NamedOnly("numbers")));
        ParsingException e = expectThrows(ParsingException.class, () -> registry.resolve(Source.EMPTY, "quasar"));
        assertThat(e.getMessage(), containsString("unknown source function [quasar], expected one of [letters, numbers]"));
    }

    public void testDuplicateNameIsRejected() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new SourceFunctionRegistry(List.of(new NamedOnly("thing"), new NamedOnly("thing")))
        );
        assertThat(e.getMessage(), containsString("duplicate source function [thing]"));
    }

    public void testNameMustBeLowercase() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new SourceFunctionRegistry(List.of(new NamedOnly("Thing")))
        );
        assertThat(e.getMessage(), containsString("source function name [Thing] must be lowercase"));
    }

    /**
     * A name no query can lex is refused where it is registered. Nothing downstream would refuse it: the token is
     * {@code LETTER UNQUOTED_ID_BODY* '('}, so each of these lexes as an index pattern followed by a stray
     * {@code (} and the function would sit in the registry unreachable rather than fail anywhere a reader looks.
     */
    public void testNameMustBeOneAQueryCanLex() {
        for (String name : List.of("_thing", "1thing", "ex-ample", "ex.ample", "ex ample")) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new SourceFunctionRegistry(List.of(new NamedOnly(name)))
            );
            assertThat(e.getMessage(), containsString("source function name [" + name + "] must start with a letter"));
        }
    }

    /**
     * The snapshot split, which is the scalar registry's: a snapshot-only function is registered where the build
     * allows it and simply absent where it does not. Both arms, with the build's answer passed in, because the
     * release arm is the one a test run on a snapshot build would otherwise never reach -- and the release arm is
     * the whole point of having the split.
     */
    public void testASnapshotOnlyFunctionIsRegisteredOnlyOnASnapshotBuild() {
        SourceFunction released = new NamedOnly("always");
        SourceFunction snapshotOnly = new NamedOnly("someday");

        SourceFunctionRegistry onSnapshot = new SourceFunctionRegistry(List.of(released), List.of(snapshotOnly), true);
        assertSame(snapshotOnly, onSnapshot.resolve(Source.EMPTY, "someday"));
        assertSame(released, onSnapshot.resolve(Source.EMPTY, "always"));

        SourceFunctionRegistry onRelease = new SourceFunctionRegistry(List.of(released), List.of(snapshotOnly), false);
        assertSame(released, onRelease.resolve(Source.EMPTY, "always"));
        // Absent rather than refused, so the answer is the one any unregistered name gets. No second message.
        ParsingException e = expectThrows(ParsingException.class, () -> onRelease.resolve(Source.EMPTY, "someday"));
        assertThat(e.getMessage(), containsString("unknown source function [someday], expected one of [always]"));
    }

    /** And the declaration is readable either way, so a caller can say why a name is unavailable. */
    public void testWhetherANameIsSnapshotOnlyIsTheSameAnswerOnEitherBuild() {
        List<SourceFunction> released = List.of(new NamedOnly("always"));
        List<SourceFunction> snapshotOnly = List.of(new NamedOnly("someday"));
        for (boolean snapshotBuild : List.of(true, false)) {
            SourceFunctionRegistry registry = new SourceFunctionRegistry(released, snapshotOnly, snapshotBuild);
            assertTrue("someday on snapshotBuild=" + snapshotBuild, registry.isSnapshotOnly("someday"));
            assertTrue("case-insensitively", registry.isSnapshotOnly("SOMEDAY"));
            assertFalse("always on snapshotBuild=" + snapshotBuild, registry.isSnapshotOnly("always"));
            assertFalse("an unregistered name", registry.isSnapshotOnly("never_declared"));
        }
    }

    /**
     * A name declared in both lists is a mistake rather than a precedence rule, and it is caught on a release
     * build too -- where the snapshot copy is never registered, so a check done at registration would miss it.
     */
    public void testANameInBothListsIsRejectedOnEitherBuild() {
        for (boolean snapshotBuild : List.of(true, false)) {
            IllegalStateException e = expectThrows(
                IllegalStateException.class,
                () -> new SourceFunctionRegistry(List.of(new NamedOnly("twice")), List.of(new NamedOnly("twice")), snapshotBuild)
            );
            assertThat(e.getMessage(), containsString("duplicate source function [twice]"));
        }
    }

    /**
     * And the name checks run over the snapshot list on a release build as well, for the same reason: a function
     * nobody registers today still has to be one a query could lex when it is released.
     */
    public void testASnapshotOnlyFunctionsNameIsCheckedOnAReleaseBuild() {
        IllegalStateException e = expectThrows(
            IllegalStateException.class,
            () -> new SourceFunctionRegistry(List.of(), List.of(new NamedOnly("_leading_underscore")), false)
        );
        assertThat(e.getMessage(), containsString("must start with a letter"));
    }

    /** And the shapes that do lex are accepted, so the check is not refusing everything. */
    public void testALexableNameIsAccepted() {
        for (String name : List.of("thing", "thing2", "thing_two", "t")) {
            assertThat(new SourceFunctionRegistry(List.of(new NamedOnly(name))).resolve(Source.EMPTY, name).name(), equalTo(name));
        }
    }

    public void testRegistryStartsEmpty() {
        // An empty registry answers without listing an empty set, which would read as an engine fault rather
        // than as an answer about the name that was asked for.
        ParsingException e = expectThrows(ParsingException.class, () -> new SourceFunctionRegistry(List.of()).resolve(Source.EMPTY, "x"));
        assertThat(e.getMessage(), containsString("no source functions are registered, so [x] cannot be used here"));
    }

}
