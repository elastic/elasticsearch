package org.elasticsearch.common.settings;

import org.elasticsearch.common.component.AbstractComponent;
import org.elasticsearch.common.inject.Inject;

import java.util.concurrent.CopyOnWriteArrayList;

/**
 *
 */
public class SettingsFilter extends AbstractComponent {

    public static interface Filter {

        void filter(ImmutableSettings.Builder settings);
    }

    private final CopyOnWriteArrayList<Filter> filters = new CopyOnWriteArrayList<Filter>();

    @Inject
    public SettingsFilter(Settings settings) {
        super(settings);
    }

    public void addFilter(Filter filter) {
        filters.add(filter);
    }

    public void removeFilter(Filter filter) {
        filters.remove(filter);
    }

    public Settings filterSettings(Settings settings) {
        ImmutableSettings.Builder builder = ImmutableSettings.settingsBuilder().put(settings);
        for (Filter filter : filters) {
            filter.filter(builder);
        }
        return builder.build();
    }
}
