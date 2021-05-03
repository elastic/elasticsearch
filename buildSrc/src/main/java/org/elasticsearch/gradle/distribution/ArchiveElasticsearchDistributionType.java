/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0 and the Server Side Public License, v 1; you may not use this file except
 * in compliance with, at your election, the Elastic License 2.0 or the Server
 * Side Public License, v 1.
 */

package org.elasticsearch.gradle.distribution;

import org.elasticsearch.gradle.ElasticsearchDistributionType;

public class ArchiveElasticsearchDistributionType implements ElasticsearchDistributionType {

    ArchiveElasticsearchDistributionType() {}

    @Override
    public String getName() {
        return "archive";
    }

    @Override
    public boolean isIntegTestZip() {
        return false;
    }

    @Override
    public boolean isArchive() {
        return true;
    }

    @Override
    public boolean isRpm() {
        return false;
    }

    @Override
    public boolean isDeb() {
        return false;
    }

    @Override
    public boolean isDocker() {
        return false;
    }

    @Override
    public boolean isDockerUbi() {
        return false;
    }

    @Override
    public boolean isDockerIronBank() {
        return false;
    }

    @Override
    public boolean shouldExtract() {
        return true;
    }

    @Override
    public boolean isDockerBased() {
        return false;
    }

}
