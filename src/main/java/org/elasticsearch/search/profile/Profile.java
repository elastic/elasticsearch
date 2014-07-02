package org.elasticsearch.search.profile;


import org.apache.lucene.search.Query;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.common.io.stream.Streamable;
import org.elasticsearch.common.lucene.search.profile.ProfileQuery;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.common.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * This class represents the aggregated/collapsed results of a profiled query (e.g. a query wrapped in
 * one or more ProfileQuery queries).  Since queries are not serializable and contain non-profiling
 * components, it needs to be "collapsed" into a common data structure, which the Profile object
 * represents.
 *
 * Profiles may have zero or more children "components", which themselves are Profile objects.  Each
 * Profile objects holds a "time" value which represents the total aggregate time at that level
 * in the tree.
 */
public class Profile implements Streamable, ToXContent {

    // Profiles may have zero or more components (e.g. a profiled Bool may have several components)
    private ArrayList<Profile> components;

    // The short class name (e.g. TermQuery)
    private String className;

    // The Lucene toString() of the class (e.g. "my_field:zach")
    private String details;

    // Aggregate time for this Profile.  Includes timing of children components
    private long time;

    // Total time of the entire Profile tree.  Provided by the parent, used to calculate relative timing
    private long totalTime;

    public Profile(ProfileQuery pQuery) {
        this();
        components.add(Profile.collapse(pQuery));
    }

    public Profile() {
        this.components = new ArrayList<>();
    }

    public void setClassName(String name) {
        this.className = name;
    }

    public void setDetails(String details) {
        this.details = details;
    }

    public void addComponent(Profile child) {
        this.components.add(child);
    }

    public ArrayList<Profile> getComponents() {
        return this.components;
    }

    public String getClassName() {
        return this.className;
    }

    public String getLuceneDetails() {
        return this.details;
    }

    public long time() {
        return time;
    }

    public void setTime(long time) {
        this.time = time;
    }

    public long totalTime() {
        return totalTime;
    }

    public void setTotalTime(long time) { this.totalTime = time; }

    /**
     * Merge another Profile with this one.  The combined results are merged
     * into this Profile, not `other`
     *
     * @param other Another Profile to merge
     * @return this
     */
    public Profile merge(Profile other) {
        if (components.size() > 0) {
            for (int i = 0; i < this.components.size(); ++i) {

                if (other.components != null && i < other.components.size()) {
                    components.set(i, components.get(i).merge(other.components.get(i)));
                }
            }
        }
        this.time += other.time();

        return this;
    }


    /**
     * ProfileQueries store their times internally, and nest inside each other like normal queries.
     * To extract/aggregate and serialize this data between shards, we need to collapse it down to a
     * dedicated Profile object
     *
     * @param pQuery A Query that (presumably) contains at least one ProfileQuery
     * @return Returns a Profile object that represents the just the ProfileQuery components of a query
     */
    public static Profile collapse(Query pQuery) {
        ProfileCollapsingVisitor walker = new ProfileCollapsingVisitor();
        return (Profile) walker.apply(pQuery).get(0);
    }

    /**
     * Merge two or more Profile objects into a single Profile.  This combines the timing scores.
     * Profiles *must* have identical structure or else results will potentially omit paths
     * through the three
     *
     * @param profiles list of profiles to merge
     * @return         Single Profile object representing the merged set
     */
    public static Profile merge(Profile... profiles) {

        if (profiles.length == 0) {
            throw new ElasticsearchException("Cannot merge zero profiles together.");
        }

        Profile finalProfile = null;
        for (Profile p : profiles) {
            if (finalProfile == null) {
                finalProfile = p;
            } else {
                finalProfile = finalProfile.merge(p);
            }
        }

        // finalProfile cannot be null here, since we abort earlier if profiles.len == 0
        finalProfile.setTotalTime(finalProfile.time());

        return finalProfile;
    }

    public static Profile readProfile(StreamInput in) throws IOException {
        Profile result = new Profile();
        result.readFrom(in);
        return result;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.startObject();
        builder.field("type", className);
        builder.field("time", time);
        builder.field("relative", String.format("%.5g%%", ((float) time / (float)totalTime)*100f));
        builder.field("lucene", details);

        if (components.size() > 0) {
            builder.startArray("components");
            for (Profile component : components) {
                component.setTotalTime(totalTime);
                component.toXContent(builder, params);
            }
            builder.endArray();
        }
        builder.endObject();

        return builder;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        className = in.readString();
        time = in.readLong();
        totalTime = in.readLong();
        details = in.readString();

        int componentSize = in.readInt();
        components = new ArrayList<Profile>(componentSize);
        for (int i = 0; i < componentSize; ++i) {
            Profile p = new Profile();
            p.readFrom(in);
            components.add(p);
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(className);
        out.writeLong(time);
        out.writeLong(totalTime);
        out.writeString(details);

        if (components.size() > 0) {
            out.writeInt(components.size());
            for (Profile component : components) {
                component.writeTo(out);
            }
        } else {
            out.writeInt(0);
        }

    }
}
