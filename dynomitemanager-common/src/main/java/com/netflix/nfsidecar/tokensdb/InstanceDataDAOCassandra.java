package com.netflix.nfsidecar.tokensdb;

import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.google.common.base.Supplier;
import com.google.inject.Inject;
import com.google.inject.Singleton;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.Host;
import com.netflix.astyanax.connectionpool.NodeDiscoveryType;
import com.netflix.astyanax.connectionpool.OperationResult;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolConfigurationImpl;
import com.netflix.astyanax.connectionpool.impl.ConnectionPoolType;
import com.netflix.astyanax.connectionpool.impl.CountingConnectionPoolMonitor;
import com.netflix.astyanax.impl.AstyanaxConfigurationImpl;
import com.netflix.astyanax.model.*;
import com.netflix.astyanax.serializers.StringSerializer;
import com.netflix.astyanax.thrift.ThriftFamilyFactory;
import com.netflix.astyanax.util.TimeUUIDUtils;
import com.netflix.nfsidecar.config.CassCommonConfig;
import com.netflix.nfsidecar.config.CommonConfig;
import com.netflix.nfsidecar.identity.AppsInstance;
import com.netflix.nfsidecar.supplier.HostSupplier;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Singleton
public class InstanceDataDAOCassandra {
    private static final Logger logger = LoggerFactory.getLogger(InstanceDataDAOCassandra.class);

    private String cnId = "Id";
    private String cnAppid = "appId";
    private String cnAz = "availabilityZone";
    private String cnDc = "datacenter";
    private String cnInstanceid = "instanceId";
    private String cnHostname = "hostname";
    private String cnDynomitePort = "dynomitePort";
    private String cnDynomiteSecurePort = "dynomiteSecurePort";
    private String cnDynomiteSecureStoragePort = "dynomiteSecureStoragePort";
    private String cnPeerPort = "peerPort";
    private String cnEip = "elasticIP";
    private String cnToken = "token";
    private String cnLocation = "location";
    private String cnVolumePrefix = "ssVolumes";
    private String cnUpdatetime = "updatetime";
    private String cfNameTokens = "tokens";
    private String cfNameLocks = "locks";

    private final Keyspace bootKeyspace;
    private final CommonConfig commonConfig;
    private final CassCommonConfig cassCommonConfig;
    private final HostSupplier hostSupplier;
    private final String bootCluster;
    private final String ksName;
    private final int thriftPortForAstyanax;
    private final AstyanaxContext<Keyspace> ctx;
    private long lastTimeCassandraPull;
    private Set<AppsInstance> appInstances;
    private final ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock read  = readWriteLock.readLock();
    private final Lock write = readWriteLock.writeLock();
    /*
     * Schema: create column family tokens with comparator=UTF8Type and
     * column_metadata=[ {column_name: appId, validation_class:
     * UTF8Type,index_type: KEYS}, {column_name: instanceId, validation_class:
     * UTF8Type}, {column_name: token, validation_class: UTF8Type},
     * {column_name: availabilityZone, validation_class: UTF8Type},
     * {column_name: hostname, validation_class: UTF8Type},{column_name: Id,
     * validation_class: UTF8Type}, {column_name: elasticIP, validation_class:
     * UTF8Type}, {column_name: updatetime, validation_class: TimeUUIDType},
     * {column_name: location, validation_class: UTF8Type}];
     */
    public ColumnFamily<String, String> CF_TOKENS = new ColumnFamily<>(cfNameTokens,
            StringSerializer.get(), StringSerializer.get());
    // Schema: create column family locks with comparator=UTF8Type;
    public ColumnFamily<String, String> CF_LOCKS = new ColumnFamily<>(cfNameLocks,
            StringSerializer.get(), StringSerializer.get());

    @Inject
    public InstanceDataDAOCassandra(CommonConfig commonConfig, CassCommonConfig cassCommonConfig, HostSupplier hostSupplier) throws ConnectionException {
        this.cassCommonConfig = cassCommonConfig;
        this.commonConfig = commonConfig;

        bootCluster = cassCommonConfig.getCassandraClusterName();

        if (bootCluster == null || bootCluster.isEmpty()) {
            throw new RuntimeException(
                    "Cassandra cluster name cannot be blank. Please use getCassandraClusterName() property.");
        }

        ksName = cassCommonConfig.getCassandraKeyspaceName();

        if (ksName == null || ksName.isEmpty()) {
            throw new RuntimeException(
                    "Cassandra Keyspace can not be blank. Please use getCassandraKeyspaceName() property.");
        }

        thriftPortForAstyanax = cassCommonConfig.getCassandraThriftPort();
        if (thriftPortForAstyanax <= 0) {
            throw new RuntimeException(
                    "Thrift Port for Astyanax can not be blank. Please use getCassandraThriftPort() property.");
        }

        this.hostSupplier = hostSupplier;

        if (cassCommonConfig.isEurekaHostsSupplierEnabled()) {
            ctx = initWithThriftDriverWithEurekaHostsSupplier();
        } else {
            ctx = initWithThriftDriverWithExternalHostsSupplier();
        }

        ctx.start();
        bootKeyspace = ctx.getClient();
    }
    private boolean isCassandraCacheExpired() {
        return lastTimeCassandraPull + cassCommonConfig.getTokenRefreshInterval() <= System.currentTimeMillis();
    }

    public void createInstanceEntry(AppsInstance instance) throws Exception {
        logger.info("*** Creating New Instance Entry ***");
        String key = getRowKey(instance);
        // If the key exists throw exception
        if (getInstance(instance.getApp(), instance.getRack(), instance.getId()) != null) {
            logger.info(String.format("Key already exists: %s", key));
            return;
        }

        getLock(instance);

        try {
            MutationBatch m = bootKeyspace.prepareMutationBatch();
            ColumnListMutation<String> clm = m.withRow(CF_TOKENS, key);
            clm.putColumn(cnId, Integer.toString(instance.getId()), null);
            clm.putColumn(cnAppid, instance.getApp(), null);
            clm.putColumn(cnAz, instance.getZone(), null);
            clm.putColumn(cnDc, commonConfig.getRack(), null);
            clm.putColumn(cnInstanceid, instance.getInstanceId(), null);
            clm.putColumn(cnHostname, instance.getHostName(), null);
            clm.putColumn(cnDynomitePort, Integer.toString(instance.getDynomitePort()), null);
            clm.putColumn(cnDynomiteSecurePort, Integer.toString(instance.getDynomiteSecurePort()), null);
            clm.putColumn(cnDynomiteSecureStoragePort, Integer.toString(instance.getDynomiteSecureStoragePort()), null);
            clm.putColumn(cnPeerPort, Integer.toString(instance.getPeerPort()), null);
            clm.putColumn(cnEip, instance.getHostIP(), null);
            clm.putColumn(cnToken, instance.getToken(), null);
            clm.putColumn(cnLocation, instance.getDatacenter(), null);
            clm.putColumn(cnUpdatetime, TimeUUIDUtils.getUniqueTimeUUIDinMicros(), null);
            Map<String, Object> volumes = instance.getVolumes();
            if (volumes != null) {
                for (String path : volumes.keySet()) {
                    clm.putColumn(cnVolumePrefix + "_" + path, volumes.get(path).toString(), null);
                }
            }
            m.execute();
        } catch (Exception e) {
            logger.info(e.getMessage());
        } finally {
            releaseLock(instance);
        }
    }

    /*
     * To get a lock on the row - Create a choosing row and make sure there are
     * no contenders. If there are bail out. Also delete the column when bailing
     * out. - Once there are no contenders, grab the lock if it is not already
     * taken.
     */
    private void getLock(AppsInstance instance) throws Exception {

        String choosingkey = getChoosingKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_LOCKS, choosingkey);

        // Expire in 6 sec
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), Integer.valueOf(6));
        m.execute();
        int count = bootKeyspace.prepareQuery(CF_LOCKS).getKey(choosingkey).getCount().execute().getResult();
        if (count > 1) {
            // Need to delete my entry
            m.withRow(CF_LOCKS, choosingkey).deleteColumn(instance.getInstanceId());
            m.execute();
            throw new Exception(String.format("More than 1 contender for lock %s %d", choosingkey, count));
        }

        String lockKey = getLockingKey(instance);
        OperationResult<ColumnList<String>> result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() > 0
                && !result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            throw new Exception(String.format("Lock already taken %s", lockKey));
        }

        clm = m.withRow(CF_LOCKS, lockKey);
        clm.putColumn(instance.getInstanceId(), instance.getInstanceId(), Integer.valueOf(600));
        m.execute();
        Thread.sleep(100);
        result = bootKeyspace.prepareQuery(CF_LOCKS).getKey(lockKey).execute();
        if (result.getResult().size() == 1
                && result.getResult().getColumnByIndex(0).getName().equals(instance.getInstanceId())) {
            logger.info("Got lock " + lockKey);
            return;
        } else {
            throw new Exception(String.format("Cannot insert lock %s", lockKey));
        }

    }

    private void releaseLock(AppsInstance instance) throws Exception {
        String choosingkey = getChoosingKey(instance);
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        ColumnListMutation<String> clm = m.withRow(CF_LOCKS, choosingkey);

        m.withRow(CF_LOCKS, choosingkey).deleteColumn(instance.getInstanceId());
        m.execute();
    }

    public void deleteInstanceEntry(AppsInstance instance) throws Exception {
        // Acquire the lock first
        getLock(instance);

        // Delete the row
        String key = findKey(instance.getApp(), String.valueOf(instance.getId()), instance.getDatacenter(),
                instance.getRack());
        if (key == null) {
            return; // don't fail it

        }
        MutationBatch m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_TOKENS, key).delete();
        m.execute();

        key = getLockingKey(instance);
        // Delete key
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

        // Have to delete choosing key as well to avoid issues with delete
        // followed by immediate writes
        key = getChoosingKey(instance);
        m = bootKeyspace.prepareMutationBatch();
        m.withRow(CF_LOCKS, key).delete();
        m.execute();

    }

    public AppsInstance getInstance(String app, String rack, int id) {
        Set<AppsInstance> set = getAllInstances(app);
        for (AppsInstance ins : set) {
            if (ins.getId() == id && ins.getRack().equals(rack)) {
                return ins;
            }
        }
        return null;
    }

    public Set<AppsInstance> getLocalDCInstances(String app, String region) {
        Set<AppsInstance> set = getAllInstances(app);
        Set<AppsInstance> returnSet = new HashSet<>();

        for (AppsInstance ins : set) {
            if (ins.getDatacenter().equals(region)) {
                returnSet.add(ins);
            }
        }
        return returnSet;
    }

    public Set<AppsInstance> getAllInstancesFromCassandra(String app) {
        Set<AppsInstance> set = new HashSet<>();
        try {

            final String selectClause = String.format(
                    "SELECT * FROM %s USING CONSISTENCY LOCAL_QUORUM WHERE %s = '%s' ", cfNameTokens, cnAppid, app);
            logger.debug(selectClause);

            final ColumnFamily<String, String> cfTokensNew = ColumnFamily.newColumnFamily(ksName,
                    StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(cfTokensNew)
                    .withCql(selectClause).execute();

            for (Row<String, String> row : result.getResult().getRows())
                set.add(transform(row.getColumns()));
        } catch (Exception e) {
            logger.warn("Caught an Unknown Exception during reading msgs ... -> " + e.getMessage());
            throw new RuntimeException(e);
        }
        return set;
    }

    public Set<AppsInstance> getAllInstances(String app) {
        if (isCassandraCacheExpired() || appInstances.isEmpty()) {
            write.lock();
            if (isCassandraCacheExpired() || appInstances.isEmpty()) {
                logger.debug("lastpull %d msecs ago, getting instances from C*", System.currentTimeMillis() - lastTimeCassandraPull);
                appInstances = getAllInstancesFromCassandra(app);
                lastTimeCassandraPull = System.currentTimeMillis();
            }
            write.unlock();
        }
        read.lock();
        Set<AppsInstance> retInstances = appInstances;
        read.unlock();
        return retInstances;
    }

    public String findKey(String app, String id, String location, String datacenter) {
        try {
            final String selectClause = String.format(
                    "SELECT * FROM %s USING CONSISTENCY LOCAL_QUORUM WHERE %s = '%s' and %s = '%s' and %s = '%s' and %s = '%s' ",
                    "tokens", cnAppid, app, cnId, id, cnLocation, location, cnDc, datacenter);
            logger.info(selectClause);

            final ColumnFamily<String, String> cfInstancesNew = ColumnFamily.newColumnFamily(ksName,
                    StringSerializer.get(), StringSerializer.get());

            OperationResult<CqlResult<String, String>> result = bootKeyspace.prepareQuery(cfInstancesNew)
                    .withCql(selectClause).execute();

            if (result == null || result.getResult().getRows().size() == 0) {
                return null;
            }

            Row<String, String> row = result.getResult().getRows().getRowByIndex(0);
            return row.getKey();

        } catch (Exception e) {
            logger.warn("Caught an Unknown Exception during find a row matching cluster[" + app + "], id[" + id
                    + "], and region[" + datacenter + "]  ... -> " + e.getMessage());
            throw new RuntimeException(e);
        }

    }

    private AppsInstance transform(ColumnList<String> columns) {
        AppsInstance ins = new AppsInstance();
        Map<String, String> cmap = new HashMap<>();
        for (Column<String> column : columns) {
            // logger.info("***Column Name = "+column.getName()+ " Value =
            // "+column.getStringValue());
            cmap.put(column.getName(), column.getStringValue());
            if (column.getName().equals(cnAppid)) {
                ins.setUpdatetime(column.getTimestamp());
            }
        }

        ins.setApp(cmap.get(cnAppid));
        ins.setZone(cmap.get(cnAz));
        ins.setHost(cmap.get(cnHostname));
        ins.setDynomitePort(cmap.get(cnDynomitePort) != null ? Integer.parseInt(cmap.get(cnDynomitePort)) : commonConfig.getDynomitePort());
        ins.setDynomiteSecurePort(cmap.get(cnDynomiteSecurePort) != null ? Integer.parseInt(cmap.get(cnDynomiteSecurePort)) : commonConfig.getDynomiteSecurePort());
        ins.setDynomiteSecureStoragePort(cmap.get(cnDynomiteSecureStoragePort) != null ? Integer.parseInt(cmap.get(cnDynomiteSecureStoragePort)) : commonConfig.getDynomiteSecureStoragePort());
        ins.setPeerPort(cmap.get(cnPeerPort) != null ? Integer.parseInt(cmap.get(cnPeerPort)) : commonConfig.getDynomitePeerPort());
        ins.setHostIP(cmap.get(cnEip));
        ins.setId(Integer.parseInt(cmap.get(cnId)));
        ins.setInstanceId(cmap.get(cnInstanceid));
        ins.setDatacenter(cmap.get(cnLocation));
        ins.setRack(cmap.get(cnDc));
        ins.setToken(cmap.get(cnToken));
        return ins;
    }

    private String getChoosingKey(AppsInstance instance) {
        return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId() + "-choosing";
    }

    private String getLockingKey(AppsInstance instance) {
        return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId() + "-lock";
    }

    private String getRowKey(AppsInstance instance) {
        return instance.getApp() + "_" + instance.getRack() + "_" + instance.getId();
    }

    private AstyanaxContext<Keyspace> initWithThriftDriverWithEurekaHostsSupplier() {

        logger.info("BOOT_CLUSTER = {}, KS_NAME = {}", bootCluster, ksName);
        return new AstyanaxContext.Builder().forCluster(bootCluster).forKeyspace(ksName)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setMaxConnsPerHost(3).setPort(thriftPortForAstyanax))
                .withHostSupplier(hostSupplier.getSupplier(bootCluster))
                .withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    }

    private AstyanaxContext<Keyspace> initWithThriftDriverWithExternalHostsSupplier() {

        logger.info("BOOT_CLUSTER = {}, KS_NAME = {}", bootCluster, ksName);
        return new AstyanaxContext.Builder().forCluster(bootCluster).forKeyspace(ksName)
                .withAstyanaxConfiguration(
                        new AstyanaxConfigurationImpl().setDiscoveryType(NodeDiscoveryType.DISCOVERY_SERVICE)
                                .setConnectionPoolType(ConnectionPoolType.ROUND_ROBIN))
                .withConnectionPoolConfiguration(new ConnectionPoolConfigurationImpl("MyConnectionPool")
                        .setMaxConnsPerHost(3).setPort(thriftPortForAstyanax))
                .withHostSupplier(getSupplier()).withConnectionPoolMonitor(new CountingConnectionPoolMonitor())
                .buildKeyspace(ThriftFamilyFactory.getInstance());

    }

    private Supplier<List<Host>> getSupplier() {

        return new Supplier<List<Host>>() {

            @Override
            public List<Host> get() {

                List<Host> hosts = new ArrayList<>();

                List<String> cassHostnames = new ArrayList<>(
                        Arrays.asList(StringUtils.split(cassCommonConfig.getCassandraSeeds(), ",")));

                if (cassHostnames.isEmpty()) {
                    throw new RuntimeException(
                            "Cassandra Host Names can not be blank. At least one host is needed. Please use getCassandraSeeds() property.");
                }

                for (String cassHost : cassHostnames) {
                    logger.info("Adding Cassandra Host = {}", cassHost);
                    hosts.add(new Host(cassHost, thriftPortForAstyanax));
                }

                return hosts;
            }
        };
    }
}
