package org.elasticsearch.rest.compat;

import com.carrotsearch.randomizedtesting.annotations.Name;
import org.elasticsearch.test.rest.yaml.ClientYamlTestCandidate;

/**
 * Warning - temporary implementation. This will be replaced soon.
 */
public class CompatRestIT extends AbstractCompatRestTest {

    public CompatRestIT(@Name("yaml") ClientYamlTestCandidate testCandidate) {
        super(testCandidate);
    }

}
