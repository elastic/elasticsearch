package org.elasticsearch.protocol.xpack.ml.job.stats;

import org.elasticsearch.common.Nullable;
import org.elasticsearch.common.ParseField;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.common.xcontent.ConstructingObjectParser;
import org.elasticsearch.common.xcontent.ObjectParser;
import org.elasticsearch.common.xcontent.ToXContentObject;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.protocol.xpack.ml.job.config.Job;
import org.elasticsearch.protocol.xpack.ml.job.config.JobState;
import org.elasticsearch.protocol.xpack.ml.job.process.DataCounts;
import org.elasticsearch.protocol.xpack.ml.job.process.ModelSizeStats;
import org.elasticsearch.protocol.xpack.ml.NodeAttributes;

import java.io.IOException;
import java.util.Objects;

public class JobStats implements ToXContentObject {

    private static final ParseField DATA_COUNTS = new ParseField("data_counts");
    private static final ParseField MODEL_SIZE_STATS = new ParseField("model_size_stats");
    private static final ParseField FORECASTS_STATS = new ParseField("forecasts_stats");
    private static final ParseField STATE = new ParseField("state");
    private static final ParseField NODE = new ParseField("node");
    private static final ParseField OPEN_TIME = new ParseField("open_time");
    private static final ParseField ASSIGNMENT_EXPLANATION = new ParseField("assignment_explanation");

    public static final ConstructingObjectParser<JobStats, Void> PARSER =
        new ConstructingObjectParser<>("job_stats",
            true,
            (a) -> {
                int i = 0;
                String jobId = (String) a[i++];
                DataCounts dataCounts = (DataCounts) a[i++];
                JobState jobState = (JobState) a[i++];
                ModelSizeStats.Builder modelSizeStatsBuilder = (ModelSizeStats.Builder) a[i++];
                ModelSizeStats modelSizeStats = modelSizeStatsBuilder == null ? null : modelSizeStatsBuilder.build();
                ForecastStats forecastStats = (ForecastStats) a[i++];
                NodeAttributes node = (NodeAttributes) a[i++];
                String assignmentExplanation = (String) a[i++];
                TimeValue openTime = (TimeValue) a[i];
                return new JobStats(jobId,
                    dataCounts,
                    jobState,
                    modelSizeStats,
                    forecastStats,
                    node,
                    assignmentExplanation,
                    openTime);
            });

    static {
        PARSER.declareString(ConstructingObjectParser.constructorArg(), Job.ID);
        PARSER.declareObject(ConstructingObjectParser.constructorArg(), DataCounts.PARSER, DATA_COUNTS);
        PARSER.declareField(ConstructingObjectParser.constructorArg(),
            (p) -> JobState.fromString(p.text()),
            STATE,
            ObjectParser.ValueType.VALUE);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ModelSizeStats.PARSER, MODEL_SIZE_STATS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), ForecastStats.PARSER, FORECASTS_STATS);
        PARSER.declareObject(ConstructingObjectParser.optionalConstructorArg(), NodeAttributes.PARSER, NODE);
        PARSER.declareString(ConstructingObjectParser.optionalConstructorArg(), ASSIGNMENT_EXPLANATION);
        PARSER.declareField(ConstructingObjectParser.optionalConstructorArg(),
            (p) -> TimeValue.parseTimeValue(p.text(), OPEN_TIME.getPreferredName()),
            OPEN_TIME,
            ObjectParser.ValueType.VALUE);
    }


    private final String jobId;
    private DataCounts dataCounts;
    private JobState state;
    private ModelSizeStats modelSizeStats;
    private ForecastStats forecastStats;
    private NodeAttributes node;
    private String assignmentExplanation;
    private TimeValue openTime;

    JobStats(String jobId, DataCounts dataCounts, JobState state, @Nullable ModelSizeStats modelSizeStats,
                    @Nullable ForecastStats forecastStats, @Nullable NodeAttributes node,
                    @Nullable String assignmentExplanation, @Nullable TimeValue opentime) {
        this.jobId = Objects.requireNonNull(jobId);
        this.dataCounts = Objects.requireNonNull(dataCounts);
        this.state = Objects.requireNonNull(state);
        this.modelSizeStats = modelSizeStats;
        this.forecastStats = forecastStats;
        this.node = node;
        this.assignmentExplanation = assignmentExplanation;
        this.openTime = opentime;
    }

    public String getJobId() {
        return jobId;
    }

    public DataCounts getDataCounts() {
        return dataCounts;
    }

    public ModelSizeStats getModelSizeStats() {
        return modelSizeStats;
    }

    public ForecastStats getForecastStats() {
        return forecastStats;
    }

    public JobState getState() {
        return state;
    }

    public NodeAttributes getNode() {
        return node;
    }

    public String getAssignmentExplanation() {
        return assignmentExplanation;
    }

    public TimeValue getOpenTime() {
        return openTime;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(Job.ID.getPreferredName(), jobId);
        builder.field(DATA_COUNTS.getPreferredName(), dataCounts);
        builder.field(STATE.getPreferredName(), state.toString());
        if (modelSizeStats != null) {
            builder.field(MODEL_SIZE_STATS.getPreferredName(), modelSizeStats);
        }
        if (forecastStats != null) {
            builder.field(FORECASTS_STATS.getPreferredName(), forecastStats);
        }
        if (node != null) {
            builder.field(NODE.getPreferredName(), node);
        }
        if (assignmentExplanation != null) {
            builder.field(ASSIGNMENT_EXPLANATION.getPreferredName(), assignmentExplanation);
        }
        if (openTime != null) {
            builder.field(OPEN_TIME.getPreferredName(), openTime.getStringRep());
        }
        return builder.endObject();
    }

    @Override
    public int hashCode() {
        return Objects.hash(jobId, dataCounts, modelSizeStats, forecastStats, state, node, assignmentExplanation, openTime);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (getClass() != obj.getClass()) {
            return false;
        }
        JobStats other = (JobStats) obj;
        return Objects.equals(jobId, other.jobId)
            && Objects.equals(this.dataCounts, other.dataCounts)
            && Objects.equals(this.modelSizeStats, other.modelSizeStats)
            && Objects.equals(this.forecastStats, other.forecastStats)
            && Objects.equals(this.state, other.state)
            && Objects.equals(this.node, other.node)
            && Objects.equals(this.assignmentExplanation, other.assignmentExplanation)
            && Objects.equals(this.openTime, other.openTime);
    }
}
