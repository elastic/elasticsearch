package org.elasticsearch.xpack.eql;

import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.test.eql.EqlRestValidationTestCase;

import static org.elasticsearch.xpack.eql.SecurityUtils.secureClientSettings;

public class EqlRestValidationIT extends EqlRestValidationTestCase {

    @Override
    protected Settings restClientSettings() {
        return secureClientSettings();
    }
}
