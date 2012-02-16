package org.elasticsearch.test.unit.common.joda;

import org.elasticsearch.common.joda.DateMathParser;
import org.elasticsearch.common.joda.Joda;
import org.testng.annotations.Test;

import java.util.concurrent.TimeUnit;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

/**
 */
@Test
public class DateMathParserTests {

    @Test
    public void dataMathTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("now", 0), equalTo(0l));
        assertThat(parser.parse("now+m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+1m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("now+11m", 0), equalTo(TimeUnit.MINUTES.toMillis(11)));

        assertThat(parser.parse("now+1d", 0), equalTo(TimeUnit.DAYS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
        assertThat(parser.parse("now+1m-1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) - TimeUnit.SECONDS.toMillis(1)));

        assertThat(parser.parse("now+1m+1s/m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parseUpperInclusive("now+1m+1s/m", 0), equalTo(TimeUnit.MINUTES.toMillis(2)));
    }

    @Test
    public void actualDateTests() {
        DateMathParser parser = new DateMathParser(Joda.forPattern("dateOptionalTime"), TimeUnit.MILLISECONDS);

        assertThat(parser.parse("1970-01-01", 0), equalTo(0l));
        assertThat(parser.parse("1970-01-01||+1m", 0), equalTo(TimeUnit.MINUTES.toMillis(1)));
        assertThat(parser.parse("1970-01-01||+1m+1s", 0), equalTo(TimeUnit.MINUTES.toMillis(1) + TimeUnit.SECONDS.toMillis(1)));
    }
}
