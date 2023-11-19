/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.test.fixtures.s3;

import org.jetbrains.annotations.NotNull;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

public class S3FixtureRule implements S3Fixture {

    private boolean useFixture = true;
    private S3Fixture delegate;
    private final List<Consumer<S3Fixture>> configurationActions = new ArrayList<>();

    public S3FixtureRule(boolean useFixture) {
        this.useFixture = useFixture;
    }

    @NotNull
    @Override
    public Statement apply(@NotNull Statement base, @NotNull Description description) {
        delegate = useFixture ? new S3FixtureTestContainerRule() : new S3ExternalRule();
        delegate.apply(base, description);
        drainConfigurationActions();
        return new Statement() {
            @Override
            public void evaluate() throws Throwable {
                try {
                    base.evaluate();
                } finally {

                }
            }
        };
    }

    /**
     *
     * return new Statement() {
     *             @Override
     *             public void evaluate() throws Throwable {
     *                 S spec = specProvider.get();
     *                 try {
     *                     if (spec.isShared() == false || handle == null) {
     *                         if (spec.isShared()) {
     *                             maybeCheckThreadLeakFilters(description);
     *                         }
     *                         handle = clusterFactory.create(spec);
     *                         handle.start();
     *                     }
     *                     base.evaluate();
     *                 } finally {
     *                     if (spec.isShared() == false) {
     *                         close();
     *                     }
     *                 }
     *             }
     *         };
     * */

    private void drainConfigurationActions() {
        configurationActions.forEach((action) -> action.accept(delegate));
        configurationActions.clear();
    }

    @Override
    public S3Fixture withExposedService(String serviceName) {
        if (delegate == null) {
            this.configurationActions.add((fixture) -> fixture.withExposedService(serviceName));
        } else {
            delegate.withExposedService(serviceName);
        }
        return this;
    }

    @Override
    public String getServiceUrl(String serviceName) {
        return delegate.getServiceUrl(serviceName);
    }
}
