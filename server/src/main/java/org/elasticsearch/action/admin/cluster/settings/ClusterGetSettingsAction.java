package org.elasticsearch.action.admin.cluster.settings;

import org.elasticsearch.action.Action;

public class ClusterGetSettingsAction extends Action<ClusterGetSettingsResponse> {
    public static final ClusterGetSettingsAction INSTANCE = new ClusterGetSettingsAction();
    public static final String NAME = "cluster:admin/settings/get";

    private ClusterGetSettingsAction() {
        super(NAME);
    }

    @Override
    public ClusterGetSettingsResponse newResponse() {
        return new ClusterGetSettingsResponse();
    }
}
