package org.elasticsearch.protocol.xpack.ml;

import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.Strings;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.XContentParser;
import org.elasticsearch.protocol.xpack.ml.job.stats.JobStats;

import java.io.IOException;
import java.util.List;
import java.util.Objects;

import static org.elasticsearch.common.xcontent.ConstructingObjectParser.constructorArg;

/**
 * Contains a {@link List} of the found {@link JobStats} objects and the total count found
 */
public class GetJobsStatsResponse extends AbstractResultResponse<JobStats>  {

    public static final ParseField RESULTS_FIELD = new ParseField("jobs");

    @SuppressWarnings("unchecked")
    public static final ConstructingObjectParser<GetJobsStatsResponse, Void> PARSER =
        new ConstructingObjectParser<>("jobs_stats_response", true,
            a -> new GetJobsStatsResponse((List<JobStats>) a[0], (long) a[1]));

    static {
        PARSER.declareObjectArray(constructorArg(), JobStats.PARSER, RESULTS_FIELD);
        PARSER.declareLong(constructorArg(), AbstractResultResponse.COUNT);
    }

    GetJobsStatsResponse(List<JobStats> jobStats, long count) {
        super(RESULTS_FIELD, jobStats, count);
    }

    /**
     * The collection of {@link JobStats} objects found in the query
     */
    public List<JobStats> jobs() {
        return results;
    }

    public static GetJobsStatsResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }

    @Override
    public int hashCode() {
        return Objects.hash(results, count);
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }

        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }

        GetJobsStatsResponse other = (GetJobsStatsResponse) obj;
        return Objects.equals(results, other.results) && count == other.count;
    }

    @Override
    public final String toString() {
        return Strings.toString(this);
    }
}
