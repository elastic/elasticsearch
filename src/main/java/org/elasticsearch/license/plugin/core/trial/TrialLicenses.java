/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License;
 * you may not use this file except in compliance with the Elastic License.
 */
package org.elasticsearch.license.plugin.core.trial;

import java.util.Collection;

public interface TrialLicenses extends Iterable<TrialLicenses.TrialLicense> {

    public Collection<TrialLicense> trialLicenses();

    public TrialLicense getTrialLicense(String feature);

    public interface TrialLicense {

        public String issuedTo();

        public String feature();

        public long issueDate();

        public long expiryDate();

        public int maxNodes();

        public String uid();

    }
}
