/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.esql.action;

import org.elasticsearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.elasticsearch.action.support.IndicesOptions;
import org.elasticsearch.test.ESTestCase;

import java.util.List;

import static org.hamcrest.Matchers.equalTo;

public class EsqlResolveFieldsActionTests extends ESTestCase {

    /**
     * Every base the sweep runs, rather than one drawn at random. They differ from one another in the concrete target,
     * wildcard and gatekeeper components, so dropping any of those three on the way through reds this test. A random
     * draw would have exercised a given difference only on some seeds. All four carry the same cross-project mode,
     * which is why the sweep varies that itself rather than leaning on the base to vary it.
     */
    private static final List<IndicesOptions> BASES = List.of(
        IndicesOptions.DEFAULT,
        IndicesOptions.strictExpandOpen(),
        IndicesOptions.lenientExpandOpen(),
        IndicesOptions.strictSingleIndexNoExpandForbidClosed()
    );

    /**
     * The receiving cluster refuses to resolve its own datasets even when the caller asked it to, which is what makes a
     * remote dataset invisible to a coordinator that predates this change and still sets the flag. Nothing else on the
     * request may move: the clear is reached after the security layer has already resolved indices under the incoming
     * flag, so anything it touched beyond this one option would be a second, unrelated change to a resolved request.
     */
    public void testDatasetResolutionIsClearedWhateverTheCallerAsked() {
        for (IndicesOptions base : BASES) {
            for (boolean crossProject : new boolean[] { true, false }) {
                for (boolean aliases : new boolean[] { true, false }) {
                    for (boolean views : new boolean[] { true, false }) {
                        for (boolean callerAsked : new boolean[] { true, false }) {
                            run(base, crossProject, aliases, views, callerAsked);
                        }
                    }
                }
            }
        }
    }

    /**
     * One cell of the sweep above: hand {@code clearDatasetResolution} a request whose options are {@code base} with
     * every value it can vary set as given, and require that only the dataset flag moved. Every component is swept
     * rather than pinned, including the two the caller might have expected to be constant: a clear that switched
     * aliases or views on would agree with an expectation built from a request that already had them on.
     */
    private static void run(IndicesOptions base, boolean crossProject, boolean aliases, boolean views, boolean callerAsked) {
        var request = new FieldCapabilitiesRequest().indices("remote_employees");
        // Minted through the canonical constructors, never through IndicesOptions.Builder. The clear itself copies
        // through that builder, so an incoming value that had already been through it would arrive already missing
        // whatever the copy failed to carry, and the expectation below would read the same lossy value back.
        var incoming = new IndicesOptions(
            base.concreteTargetOptions(),
            base.wildcardOptions(),
            base.gatekeeperOptions(),
            new IndicesOptions.CrossProjectModeOptions(crossProject),
            new IndicesOptions.IndexAbstractionOptions(aliases, views, callerAsked)
        );
        request.indicesOptions(incoming);

        EsqlResolveFieldsAction.clearDatasetResolution(request);

        // The expectation is built from the canonical constructors at both levels rather than through the builders
        // the clear itself uses. Copying it through those builders would make both sides drop anything a copy
        // constructor failed to carry, and the assertion would stay green on exactly the drift it is here to
        // catch. Naming every component also makes a component added later a compile error here.
        var expected = new IndicesOptions(
            incoming.concreteTargetOptions(),
            incoming.wildcardOptions(),
            incoming.gatekeeperOptions(),
            incoming.crossProjectModeOptions(),
            new IndicesOptions.IndexAbstractionOptions(
                incoming.indexAbstractionOptions().resolveAliases(),
                incoming.indexAbstractionOptions().resolveViews(),
                false
            )
        );
        assertThat(request.indicesOptions(), equalTo(expected));
        assertThat(request.indicesOptions().indexAbstractionOptions().resolveDatasets(), equalTo(false));
        assertThat(request.indices(), equalTo(new String[] { "remote_employees" }));
    }

}
