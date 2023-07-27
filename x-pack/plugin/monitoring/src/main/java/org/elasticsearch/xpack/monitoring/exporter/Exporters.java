/*
 * Copyright Elasticsearch B.V. and/or licensed to Elasticsearch B.V. under one
 * or more contributor license agreements. Licensed under the Elastic License
 * 2.0; you may not use this file except in compliance with the Elastic License
 * 2.0.
 */
package org.elasticsearch.xpack.monitoring.exporter;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.util.Supplier;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.cluster.ClusterState;
import org.elasticsearch.cluster.service.ClusterService;
import org.elasticsearch.common.component.AbstractLifecycleComponent;
import org.elasticsearch.common.component.Lifecycle;
import org.elasticsearch.common.settings.Setting;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.settings.SettingsException;
import org.elasticsearch.common.util.concurrent.AtomicArray;
import org.elasticsearch.common.util.concurrent.CountDown;
import org.elasticsearch.common.util.concurrent.ThreadContext;
import org.elasticsearch.gateway.GatewayService;
import org.elasticsearch.license.XPackLicenseState;
import org.elasticsearch.xpack.core.monitoring.exporter.MonitoringDoc;
import org.elasticsearch.xpack.core.ssl.SSLService;
import org.elasticsearch.xpack.monitoring.Monitoring;
import org.elasticsearch.xpack.monitoring.exporter.http.HttpExporter;
import org.elasticsearch.xpack.monitoring.exporter.local.LocalExporter;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.Collections.emptyMap;

public class Exporters extends AbstractLifecycleComponent {
    private static final Logger LOGGER = LogManager.getLogger(Exporters.class);

    private final Settings settings;
    private final Map<String, Exporter.Factory> factories;
    private final AtomicReference<Map<String, Exporter>> exporters;
    private final AtomicReference<Map<String, Exporter.Config>> disabledExporterConfigs;
    private final ClusterService clusterService;
    private final XPackLicenseState licenseState;
    private final ThreadContext threadContext;

    public Exporters(
        Settings settings,
        Map<String, Exporter.Factory> factories,
        ClusterService clusterService,
        XPackLicenseState licenseState,
        ThreadContext threadContext,
        SSLService sslService
    ) {
        this.settings = settings;
        this.factories = factories;
        this.exporters = new AtomicReference<>(emptyMap());
        this.disabledExporterConfigs = new AtomicReference<>(emptyMap());
        this.threadContext = Objects.requireNonNull(threadContext);
        this.clusterService = Objects.requireNonNull(clusterService);
        this.licenseState = Objects.requireNonNull(licenseState);

        final List<Setting.AffixSetting<?>> dynamicSettings = getSettings().stream().filter(Setting::isDynamic).toList();
        final List<Setting<?>> updateSettings = new ArrayList<Setting<?>>(dynamicSettings);
        updateSettings.add(Monitoring.MIGRATION_DECOMMISSION_ALERTS);
        clusterService.getClusterSettings().addSettingsUpdateConsumer(this::setExportersSetting, updateSettings);
        HttpExporter.registerSettingValidators(clusterService, sslService);
        // this ensures that logging is happening by adding an empty consumer per affix setting
        for (Setting.AffixSetting<?> affixSetting : dynamicSettings) {
            clusterService.getClusterSettings().addAffixUpdateConsumer(affixSetting, (s, o) -> {}, (s, o) -> {});
        }
    }

    static class InitializedExporters {
        final Map<String, Exporter> enabledExporters;
        final Map<String, Exporter.Config> disabledExporters;

        InitializedExporters(Map<String, Exporter> enabledExporters, Map<String, Exporter.Config> disabledExporters) {
            this.enabledExporters = enabledExporters;
            this.disabledExporters = disabledExporters;
        }
    }

    public void setExportersSetting(Settings exportersSetting) {
        if (this.lifecycle.started()) {
            InitializedExporters initializedExporters = initExporters(exportersSetting);
            Map<String, Exporter> updated = initializedExporters.enabledExporters;
            closeExporters(LOGGER, this.exporters.getAndSet(updated));
            this.disabledExporterConfigs.getAndSet(initializedExporters.disabledExporters);
        }
    }

    @Override
    protected void doStart() {
        InitializedExporters initializedExporters = initExporters(settings);
        this.exporters.set(initializedExporters.enabledExporters);
        this.disabledExporterConfigs.set(initializedExporters.disabledExporters);
    }

    @Override
    protected void doStop() {
        closeExporters(LOGGER, exporters.get());
    }

    @Override
    protected void doClose() {}

    public Exporter getExporter(String name) {
        return exporters.get().get(name);
    }

    /**
     * Get all enabled {@linkplain Exporter}s.
     *
     * @return Never {@code null}. Can be empty if none are enabled.
     */
    public Collection<Exporter> getEnabledExporters() {
        return exporters.get().values();
    }

    /**
     * Get all disabled {@linkplain Exporter.Config}s.
     *
     * @return Never {@code null}. Can be empty if none are disabled.
     */
    public Collection<Exporter.Config> getDisabledExporterConfigs() {
        return disabledExporterConfigs.get().values();
    }

    /**
     * Attempt to construct a one-off exporter, separate from the list of enabled exporters.
     */
    public Exporter openExporter(Exporter.Config config) {
        String name = config.name();
        String type = config.type();
        Exporter.Factory factory = factories.get(type);
        if (factory == null) {
            throw new SettingsException("unknown exporter type [" + type + "] set for exporter [" + name + "]");
        }
        return factory.create(config);
    }

    static void closeExporters(Logger logger, Map<String, Exporter> exporters) {
        for (Exporter exporter : exporters.values()) {
            try {
                exporter.close();
            } catch (Exception e) {
                logger.error((Supplier<?>) () -> "failed to close exporter [" + exporter.name() + "]", e);
            }
        }
    }

    InitializedExporters initExporters(Settings settingsToUse) {
        Set<String> singletons = new HashSet<>();
        Map<String, Exporter> exportersMap = new HashMap<>();
        Map<String, Exporter.Config> disabled = new HashMap<>();
        boolean hasDisabled = false;
        Settings exportersSettings = settingsToUse.getByPrefix("xpack.monitoring.exporters.");
        for (String name : exportersSettings.names()) {
            Settings exporterSettings = exportersSettings.getAsSettings(name);
            String type = exporterSettings.get("type");
            if (type == null) {
                throw new SettingsException("missing exporter type for [" + name + "] exporter");
            }
            Exporter.Factory factory = factories.get(type);
            if (factory == null) {
                throw new SettingsException("unknown exporter type [" + type + "] set for exporter [" + name + "]");
            }
            Exporter.Config config = new Exporter.Config(name, type, settingsToUse, clusterService, licenseState);
            if (config.enabled() == false) {
                hasDisabled = true;
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("exporter [{}/{}] is disabled", type, name);
                }
                disabled.put(config.name(), config);
                continue;
            }
            Exporter exporter = factory.create(config);
            if (exporter.isSingleton()) {
                // this is a singleton exporter, let's make sure we didn't already create one
                // (there can only be one instance of a singleton exporter)
                if (singletons.contains(type)) {
                    throw new SettingsException(
                        "multiple [" + type + "] exporters are configured. there can " + "only be one [" + type + "] exporter configured"
                    );
                }
                singletons.add(type);
            }
            exportersMap.put(config.name(), exporter);
        }

        // no exporters are configured, lets create a default local one.
        //
        // NOTE: if there are exporters configured and they're all disabled, we don't
        // fallback on the default
        //
        if (exportersMap.isEmpty() && hasDisabled == false) {
            Exporter.Config config = new Exporter.Config(
                "default_" + LocalExporter.TYPE,
                LocalExporter.TYPE,
                settingsToUse,
                clusterService,
                licenseState
            );
            exportersMap.put(config.name(), factories.get(LocalExporter.TYPE).create(config));
        }

        return new InitializedExporters(exportersMap, disabled);
    }

    /**
     * Wrap every {@linkplain Exporter}'s {@linkplain ExportBulk} in a {@linkplain ExportBulk.Compound}.
     *
     * @param listener {@code null} if no exporters are ready or available.
     */
    void wrapExportBulk(final ActionListener<ExportBulk> listener) {
        final ClusterState state = clusterService.state();

        // wait until we have a usable cluster state
        if (state.blocks().hasGlobalBlock(GatewayService.STATE_NOT_RECOVERED_BLOCK)
            || ClusterState.UNKNOWN_UUID.equals(state.metadata().clusterUUID())
            || state.version() == ClusterState.UNKNOWN_VERSION) {
            LOGGER.trace("skipping exporters because the cluster state is not loaded");

            listener.onResponse(null);
            return;
        }

        final Map<String, Exporter> exporterMap = exporters.get();

        // if no exporters are defined (which is only possible if all are defined explicitly disabled),
        // then ignore the request immediately
        if (exporterMap.isEmpty()) {
            listener.onResponse(null);
            return;
        }

        final AtomicArray<ExportBulk> accumulatedBulks = new AtomicArray<>(exporterMap.size());
        final CountDown countDown = new CountDown(exporterMap.size());

        int i = 0;

        // get every exporter's ExportBulk and, when they've all responded, respond with a wrapped version
        for (final Exporter exporter : exporterMap.values()) {
            exporter.openBulk(
                new AccumulatingExportBulkActionListener(exporter.name(), i++, accumulatedBulks, countDown, threadContext, listener)
            );
        }
    }

    /**
     * Exports a collection of monitoring documents using the configured exporters
     */
    public void export(final Collection<MonitoringDoc> docs, final ActionListener<Void> listener) throws ExportException {
        if (this.lifecycleState() != Lifecycle.State.STARTED) {
            listener.onFailure(new ExportException("Export service is not started"));
        } else if (docs != null && docs.size() > 0) {
            wrapExportBulk(listener.delegateFailureAndWrap((l, bulk) -> {
                if (bulk != null) {
                    doExport(bulk, docs, l);
                } else {
                    l.onResponse(null);
                }
            }));
        } else {
            listener.onResponse(null);
        }
    }

    /**
     * Add {@code docs} and send the {@code bulk}, then respond to the {@code listener}.
     *
     * @param bulk The bulk object to send {@code docs} through.
     * @param docs The monitoring documents to send.
     * @param listener Returns {@code null} when complete, or failure where relevant.
     */
    private static void doExport(final ExportBulk bulk, final Collection<MonitoringDoc> docs, final ActionListener<Void> listener) {
        final AtomicReference<ExportException> exceptionRef = new AtomicReference<>();

        try {
            bulk.add(docs);
        } catch (ExportException e) {
            exceptionRef.set(e);
        } finally {
            bulk.flush(ActionListener.wrap(r -> {
                if (exceptionRef.get() == null) {
                    listener.onResponse(null);
                } else {
                    listener.onFailure(exceptionRef.get());
                }
            }, (exception) -> {
                if (exceptionRef.get() != null) {
                    exception.addSuppressed(exceptionRef.get());
                }
                listener.onFailure(exception);
            }));
        }
    }

    /**
     * Return all the settings of all the exporters, no matter if HTTP or Local
     */
    public static List<Setting.AffixSetting<?>> getSettings() {
        List<Setting.AffixSetting<?>> settings = new ArrayList<>();
        settings.addAll(Exporter.getSettings());
        settings.addAll(HttpExporter.getSettings());
        settings.addAll(LocalExporter.getSettings());
        return settings;
    }

    /**
     * {@code AccumulatingExportBulkActionListener} allows us to asynchronously gather all of the {@linkplain ExportBulk}s that are
     * ready, as associated with the enabled {@linkplain Exporter}s.
     */
    static class AccumulatingExportBulkActionListener implements ActionListener<ExportBulk> {

        private final String name;
        private final int indexPosition;
        private final AtomicArray<ExportBulk> accumulatedBulks;
        private final CountDown countDown;
        private final ActionListener<ExportBulk> delegate;
        private final ThreadContext threadContext;

        AccumulatingExportBulkActionListener(
            final String name,
            final int indexPosition,
            final AtomicArray<ExportBulk> accumulatedBulks,
            final CountDown countDown,
            final ThreadContext threadContext,
            final ActionListener<ExportBulk> delegate
        ) {
            this.name = name;
            this.indexPosition = indexPosition;
            this.accumulatedBulks = accumulatedBulks;
            this.countDown = countDown;
            this.threadContext = threadContext;
            this.delegate = delegate;
        }

        @Override
        public void onResponse(final ExportBulk exportBulk) {
            if (exportBulk == null) {
                LOGGER.debug("skipping exporter [{}] as it is not ready yet", name);
            } else {
                accumulatedBulks.set(indexPosition, exportBulk);
            }

            delegateIfComplete();
        }

        @Override
        public void onFailure(Exception e) {
            LOGGER.error((Supplier<?>) () -> "exporter [" + name + "] failed to open exporting bulk", e);

            delegateIfComplete();
        }

        /**
         * Once all {@linkplain Exporter}'s have responded, whether it was success or failure, then this responds with all successful
         * {@linkplain ExportBulk}s wrapped using an {@linkplain ExportBulk.Compound} wrapper.
         */
        void delegateIfComplete() {
            if (countDown.countDown()) {
                final List<ExportBulk> bulkList = accumulatedBulks.asList();

                if (bulkList.isEmpty()) {
                    delegate.onResponse(null);
                } else {
                    delegate.onResponse(new ExportBulk.Compound(bulkList, threadContext));
                }
            }
        }

    }

}
