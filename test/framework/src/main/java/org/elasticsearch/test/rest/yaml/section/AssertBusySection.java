package org.elasticsearch.test.rest.yaml.section;

import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.XContentLocation;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.test.ESTestCase;
import org.elasticsearch.test.rest.yaml.ClientYamlTestExecutionContext;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * Represents an assert_busy section:
 *
 *   - assert_busy:
 *     - max_wait_time: "30s"
 *     - do:
 *         cluster.remote_info: {}
 *     - is_true: local.connected
 *     - match: { local.num_nodes_connected: 1}
 *
 */
public class AssertBusySection implements ExecutableSection {

    private static final TimeValue DEFAULT_MAX_WAIT_TIME = TimeValue.timeValueSeconds(10L);

    public static AssertBusySection parse(final XContentParser parser) throws IOException {
        final XContentLocation location = parser.getTokenLocation();
        if (parser.nextToken() != XContentParser.Token.START_ARRAY) {
            throw new IllegalArgumentException("expected [" + XContentParser.Token.START_ARRAY + "], " +
                "found [" + parser.currentToken() + "], the assert_busy section is not properly indented");
        }

        TimeValue maxWaitTime = DEFAULT_MAX_WAIT_TIME;
        DoSection doSection = null;
        final List<Assertion> assertions = new ArrayList<>();
        while (parser.currentToken() != XContentParser.Token.END_ARRAY) {
            ParserUtils.advanceToFieldName(parser);
            if ("max_wait_time".equals(parser.currentName())) {
                if (parser.nextToken() != XContentParser.Token.VALUE_STRING) {
                    throw new IllegalArgumentException("expected [" + XContentParser.Token.VALUE_STRING + "], " +
                        "found [" + parser.currentToken() + "], the max_wait_time value has a wrong type");
                }
                maxWaitTime = TimeValue.parseTimeValue(parser.textOrNull(), DEFAULT_MAX_WAIT_TIME, parser.currentName());
                parser.nextToken();
            } else if ("do".equals(parser.currentName())) {
                doSection = DoSection.parse(parser);
            } else {
                ExecutableSection assertion = parser.namedObject(ExecutableSection.class, parser.currentName(), null);
                if (assertion instanceof Assertion) {
                    if (assertion instanceof AssertBusySection) {
                        throw new IllegalArgumentException("section [assert_busy] not supported within assert_busy section");
                    }
                    assertions.add((Assertion) assertion);
                }
            }
            parser.nextToken();
        }
        parser.nextToken();
        return new AssertBusySection(location, maxWaitTime, doSection, assertions);
    }

    private final TimeValue maxWaitTime;
    private final XContentLocation location;
    private final DoSection doSection;
    private final List<Assertion> assertions;

    private AssertBusySection(XContentLocation location, TimeValue maxWaitTime, DoSection doSection, List<Assertion> assertions) {
        this.maxWaitTime = Objects.requireNonNull(maxWaitTime);
        this.location = Objects.requireNonNull(location);
        this.doSection = Objects.requireNonNull(doSection);
        this.assertions = Objects.requireNonNull(assertions);
        if (assertions.isEmpty()) {
            throw new IllegalArgumentException("Missing assertions in assert_busy section");
        }
    }

    @Override
    public XContentLocation getLocation() {
        return location;
    }

    TimeValue getMaxWaitTime() {
        return maxWaitTime;
    }

    DoSection getDoSection() {
        return doSection;
    }

    List<Assertion> getAssertions() {
        return assertions;
    }

    @Override
    public void execute(final ClientYamlTestExecutionContext executionContext) throws IOException {
        try {
            ESTestCase.assertBusy(() -> {
                doSection.execute(executionContext);
                for (Assertion assertion : assertions) {
                    assertion.execute(executionContext);
                }
            }, maxWaitTime.duration(), maxWaitTime.timeUnit());
        } catch (Exception e) {
            throw new IOException("Fail to execute assert_busy section", e);
        } catch (AssertionError e) {
            throw new AssertionError("assert_busy section timed out after [" + maxWaitTime + "]", e);
        }
    }
}
