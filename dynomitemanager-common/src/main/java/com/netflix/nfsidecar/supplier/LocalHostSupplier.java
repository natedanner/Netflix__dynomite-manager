package com.netflix.nfsidecar.supplier;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.nfsidecar.config.CassCommonConfig;

/**
 * Use the {@code DM_CASSANDRA_CLUSTER_SEEDS} environment variable to provide a
 * list of Cassandra hosts that contain the complete Dynomite topology.
 */
public class LocalHostSupplier implements HostSupplier {

    private static final String errMsg = "DM_CASSANDRA_CLUSTER_SEEDS cannot be empty. It must contain one or more Cassandra hosts.";
    private final CassCommonConfig config;

    @Inject
    public LocalHostSupplier(CassCommonConfig config) {
        this.config = config;
    }

    @Override
    public Supplier<List<Host>> getSupplier(String clusterName) {
        final List<Host> hosts = new ArrayList<>();

        String bootCluster = config.getCassandraClusterName();

        if (bootCluster.equals(clusterName)) {

            String seeds = System.getenv("DM_CASSANDRA_CLUSTER_SEEDS");

            if (seeds == null || "".equals(seeds)) {
                throw new RuntimeException(errMsg);
            }

            List<String> cassHostnames = new ArrayList<>(Arrays.asList(StringUtils.split(seeds, ",")));

            if (cassHostnames.isEmpty()) {
                throw new RuntimeException(errMsg);
            }

            for (String cassHost : cassHostnames) {
                hosts.add(new Host(cassHost, 9160));
            }

        } else {
            hosts.add(new Host("127.0.0.1", 9160).setRack("localdc"));
        }

        return new Supplier<List<Host>>() {
            @Override
            public List<Host> get() {
                return hosts;
            }
        };

    }

}
