/**
 * Copyright 2013 Netflix, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.netflix.nfsidecar.aws;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.autoscaling.AmazonAutoScaling;
import com.amazonaws.services.autoscaling.AmazonAutoScalingClient;
import com.amazonaws.services.autoscaling.model.AutoScalingGroup;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsRequest;
import com.amazonaws.services.autoscaling.model.DescribeAutoScalingGroupsResult;
import com.amazonaws.services.autoscaling.model.Instance;
import com.amazonaws.services.autoscaling.model.UpdateAutoScalingGroupRequest;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2Client;
import com.amazonaws.services.ec2.model.AuthorizeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsRequest;
import com.amazonaws.services.ec2.model.DescribeSecurityGroupsResult;
import com.amazonaws.services.ec2.model.Filter;
import com.amazonaws.services.ec2.model.IpPermission;
import com.amazonaws.services.ec2.model.RevokeSecurityGroupIngressRequest;
import com.amazonaws.services.ec2.model.SecurityGroup;
import com.google.common.collect.Lists;
import com.google.inject.Inject;
import com.google.inject.name.Named;
import com.netflix.nfsidecar.identity.IMembership;
import com.netflix.nfsidecar.identity.InstanceEnvIdentity;
import com.netflix.nfsidecar.instance.InstanceDataRetriever;
import com.netflix.nfsidecar.resources.env.IEnvVariables;

/**
 * Class to query amazon ASG for its members to provide - Number of valid nodes
 * in the ASG - Number of zones - Methods for adding ACLs for the nodes
 */
public class AWSMembership implements IMembership {
    private static final Logger logger = LoggerFactory.getLogger(AWSMembership.class);
    private final ICredential provider;
    private final ICredential crossAccountProvider;
    private final InstanceEnvIdentity insEnvIdentity;
    private final InstanceDataRetriever retriever;
    private final IEnvVariables envVariables;

    @Inject
    public AWSMembership(ICredential provider, @Named("awsroleassumption") ICredential crossAccountProvider,
            InstanceEnvIdentity insEnvIdentity, InstanceDataRetriever retriever, IEnvVariables envVariables) {
        this.provider = provider;
        this.crossAccountProvider = crossAccountProvider;
        this.insEnvIdentity = insEnvIdentity;
        this.retriever = retriever;
        this.envVariables = envVariables;

    }

    @Override
    public List<String> getRacMembership() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(envVariables.getRack());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances())
                    if (!("Terminating".equalsIgnoreCase(ins.getLifecycleState())
                            || "shutting-down".equalsIgnoreCase(ins.getLifecycleState())
                            || "Terminated".equalsIgnoreCase(ins.getLifecycleState()))) {
                        instanceIds.add(ins.getInstanceId());
                    }
            }
            logger.info(String.format("Querying Amazon returned following instance in the ASG: %s --> %s",
                    envVariables.getRack(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public List<String> getCrossAccountRacMembership() {
        AmazonAutoScaling client = null;
        try {
            client = getCrossAccountAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(envVariables.getRack());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);

            List<String> instanceIds = Lists.newArrayList();
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                for (Instance ins : asg.getInstances())
                    if (!("Terminating".equalsIgnoreCase(ins.getLifecycleState())
                            || "shutting-down".equalsIgnoreCase(ins.getLifecycleState())
                            || "Terminated".equalsIgnoreCase(ins.getLifecycleState()))) {
                        instanceIds.add(ins.getInstanceId());
                    }
            }
            logger.info(String.format("Querying Amazon returned following instance in the cross-account ASG: %s --> %s",
                    envVariables.getRack(), StringUtils.join(instanceIds, ",")));
            return instanceIds;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Actual membership AWS source of truth...
     */
    @Override
    public int getRacMembershipSize() {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(envVariables.getRack());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Query on ASG returning %d instances", size));
            return size;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Cross-account member of AWS
     */
    @Override
    public int getCrossAccountRacMembershipSize() {
        AmazonAutoScaling client = null;
        try {
            client = getCrossAccountAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(envVariables.getRack());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            int size = 0;
            for (AutoScalingGroup asg : res.getAutoScalingGroups()) {
                size += asg.getMaxSize();
            }
            logger.info(String.format("Query on cross account ASG returning %d instances", size));
            return size;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * Adding peers' IPs as ingress to the running instance SG. The running
     * instance could be in "classic" or "vpc"
     */
    public void addACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<>();
            ipPermissions.add(
                    new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));

            if (this.insEnvIdentity.isClassic()) {
                client.authorizeSecurityGroupIngress(
                        new AuthorizeSecurityGroupIngressRequest(envVariables.getDynomiteClusterName(), ipPermissions));
                logger.info("Done adding ACL to classic: " + StringUtils.join(listIPs, ","));
            } else {
                AuthorizeSecurityGroupIngressRequest sgIngressRequest = new AuthorizeSecurityGroupIngressRequest();
                // fetch SG group id for VPC account of the running instances
                sgIngressRequest.withGroupId(getVpcGroupId());
                // Add peer's IPs as ingress to the SG that the running instance
                // belongs to
                client.authorizeSecurityGroupIngress(sgIngressRequest.withIpPermissions(ipPermissions));
                logger.info("Done adding ACL to vpc: " + StringUtils.join(listIPs, ","));
            }

        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /*
     * @return SG group id for a group name, vpc account of the running
     * instance. ACLGroupName = Cluster name and VPC-ID is the VPC We need both
     * filters to find the SG for that cluster and that vpc-id
     */
    protected String getVpcGroupId() {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();

            Filter nameFilter = new Filter().withName("group-name").withValues(envVariables.getDynomiteClusterName()); // SG
            Filter vpcFilter = new Filter().withName("vpc-id").withValues(retriever.getVpcId());

            logger.info("Dynomite name: " + envVariables.getDynomiteClusterName());

            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups()) {
                logger.debug(String.format("got group-id:%s for group-name:%s,vpc-id:%s", group.getGroupId(),
                        envVariables.getDynomiteClusterName(), retriever.getVpcId()));
                return group.getGroupId();
            }
            logger.error(String.format("unable to get group-id for group-name=%s vpc-id=%s",
                    envVariables.getDynomiteClusterName(), retriever.getVpcId()));
            return "";
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * removes a iplist from the SG
     */
    public void removeACL(Collection<String> listIPs, int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<IpPermission> ipPermissions = new ArrayList<>();
            ipPermissions.add(
                    new IpPermission().withFromPort(from).withIpProtocol("tcp").withIpRanges(listIPs).withToPort(to));

            if (this.insEnvIdentity.isClassic()) {
                client.revokeSecurityGroupIngress(
                        new RevokeSecurityGroupIngressRequest(envVariables.getDynomiteClusterName(), ipPermissions));
                logger.info("Done removing from ACL within classic env for running instance: "
                        + StringUtils.join(listIPs, ","));
            } else {
                RevokeSecurityGroupIngressRequest req = new RevokeSecurityGroupIngressRequest();
                req.withGroupId(getVpcGroupId()); // fetch SG group id for vpc
                                                  // account of the running
                                                  // instance.
                // Adding Peer's IPs as ingress to the running instances
                client.revokeSecurityGroupIngress(req.withIpPermissions(ipPermissions));
                logger.info("Done removing from ACL within vpc env for running instance: "
                        + StringUtils.join(listIPs, ","));
            }

        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    /**
     * List SG ACL's
     */
    public List<String> listACL(int from, int to) {
        AmazonEC2 client = null;
        try {
            client = getEc2Client();
            List<String> ipPermissions = new ArrayList<>();

            Filter nameFilter = new Filter().withName("group-name").withValues(envVariables.getDynomiteClusterName());
            String vpcid = retriever.getVpcId();
            if (vpcid == null || vpcid.isEmpty()) {
                throw new IllegalStateException("vpcid is null even though instance is running in vpc.");
            }

            Filter vpcFilter = new Filter().withName("vpc-id").withValues(vpcid);
            DescribeSecurityGroupsRequest req = new DescribeSecurityGroupsRequest().withFilters(nameFilter, vpcFilter);
            DescribeSecurityGroupsResult result = client.describeSecurityGroups(req);
            for (SecurityGroup group : result.getSecurityGroups())
                for (IpPermission perm : group.getIpPermissions())
                    if (perm.getFromPort() == from && perm.getToPort() == to) {
                        ipPermissions.addAll(perm.getIpRanges());
                    }

            logger.info("Fetch current permissions for vpc env of running instance");

            return ipPermissions;
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    @Override
    public void expandRacMembership(int count) {
        AmazonAutoScaling client = null;
        try {
            client = getAutoScalingClient();
            DescribeAutoScalingGroupsRequest asgReq = new DescribeAutoScalingGroupsRequest()
                    .withAutoScalingGroupNames(envVariables.getRack());
            DescribeAutoScalingGroupsResult res = client.describeAutoScalingGroups(asgReq);
            AutoScalingGroup asg = res.getAutoScalingGroups().get(0);
            UpdateAutoScalingGroupRequest ureq = new UpdateAutoScalingGroupRequest();
            ureq.setAutoScalingGroupName(asg.getAutoScalingGroupName());
            ureq.setMinSize(asg.getMinSize() + 1);
            ureq.setMaxSize(asg.getMinSize() + 1);
            ureq.setDesiredCapacity(asg.getMinSize() + 1);
            client.updateAutoScalingGroup(ureq);
        } finally {
            if (client != null) {
                client.shutdown();
            }
        }
    }

    protected AmazonAutoScaling getAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(provider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + envVariables.getRegion() + ".amazonaws.com");
        return client;
    }

    protected AmazonAutoScaling getCrossAccountAutoScalingClient() {
        AmazonAutoScaling client = new AmazonAutoScalingClient(crossAccountProvider.getAwsCredentialProvider());
        client.setEndpoint("autoscaling." + envVariables.getRegion() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(provider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + envVariables.getRegion() + ".amazonaws.com");
        return client;
    }

    protected AmazonEC2 getCrossAccountEc2Client() {
        AmazonEC2 client = new AmazonEC2Client(crossAccountProvider.getAwsCredentialProvider());
        client.setEndpoint("ec2." + envVariables.getRegion() + ".amazonaws.com");
        return client;
    }
}