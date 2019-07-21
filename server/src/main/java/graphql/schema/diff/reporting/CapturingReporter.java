package graphql.schema.diff.reporting;

import graphql.PublicApi;
import graphql.schema.diff.DiffEvent;
import graphql.schema.diff.DiffLevel;

import java.util.ArrayList;
import java.util.List;

/**
 * A reporter that captures all the difference events as they occur
 */
@PublicApi
public class CapturingReporter implements DifferenceReporter {
    private final List<DiffEvent> events = new ArrayList<>();
    private final List<DiffEvent> infos = new ArrayList<>();
    private final List<DiffEvent> breakages = new ArrayList<>();
    private final List<DiffEvent> dangers = new ArrayList<>();

    @Override
    public void report(DiffEvent differenceEvent) {
        events.add(differenceEvent);

        if (differenceEvent.getLevel() == DiffLevel.BREAKING) {
            breakages.add(differenceEvent);
        } else if (differenceEvent.getLevel() == DiffLevel.DANGEROUS) {
            dangers.add(differenceEvent);
        } else if (differenceEvent.getLevel() == DiffLevel.INFO) {
            infos.add(differenceEvent);
        }
    }

    @Override
    public void onEnd() {
    }

    public List<DiffEvent> getEvents() {
        return new ArrayList<>(events);
    }

    public List<DiffEvent> getInfos() {
        return new ArrayList<>(infos);
    }

    public List<DiffEvent> getBreakages() {
        return new ArrayList<>(breakages);
    }

    public List<DiffEvent> getDangers() {
        return new ArrayList<>(dangers);
    }

    public int getInfoCount() {
        return infos.size();
    }

    public int getBreakageCount() {
        return breakages.size();
    }

    public int getDangerCount() {
        return dangers.size();
    }

}
