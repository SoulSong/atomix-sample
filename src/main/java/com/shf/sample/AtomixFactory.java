package com.shf.sample;

import io.atomix.cluster.Member;
import io.atomix.cluster.discovery.MulticastDiscoveryProvider;
import io.atomix.cluster.protocol.HeartbeatMembershipProtocol;
import io.atomix.core.Atomix;
import io.atomix.core.AtomixBuilder;
import io.atomix.protocols.raft.partition.RaftPartitionGroup;
import io.atomix.storage.StorageLevel;
import io.atomix.utils.net.Address;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;

import java.io.File;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.Executors;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/12/9 19:44
 */
@Slf4j
public final class AtomixFactory {
    private static final String SYSTEM = "system";
    private static final String DATA = "data";
    private static final String DATA_DIR = "/data/atomix/data";
    private static final String MANAGEMENT_DIR = "/data/atomix/management";
    private static final String WELCOME_CHANNEL = "welcome-channel";

    public static Atomix createAtomixServer(String clusterId, String memberId, String localAddress, int localPort,
                                            String groupAddress, int groupPort, int broadcastIntervalSec,
                                            int heartbeatIntervalMs, int managementGroupPartitionsSize,
                                            List<String> memberIds) {
        AtomixBuilder atomixBuilder = Atomix.builder()
                .withClusterId(clusterId)
                .withMemberId(memberId)
                .withAddress(new Address(localAddress, localPort))
                .withMulticastEnabled()
                .withMembershipProvider(MulticastDiscoveryProvider.builder()
                        .withBroadcastInterval(Duration.ofSeconds(broadcastIntervalSec))
                        .build())
                .withMembershipProtocol(HeartbeatMembershipProtocol.builder()
                        .withHeartbeatInterval(Duration.ofMillis(heartbeatIntervalMs))
                        .build())
                // The managementGroup is used to store primitive metadata and elect primaries in the raft protocol.
                .withManagementGroup(RaftPartitionGroup
                        .builder(SYSTEM)
                        // 如果不明确指定对应的成员，在出现网络分区时可能造成脑裂
                        .withMembers(memberIds)
                        .withNumPartitions(memberIds.size() / 2)
                        .withDataDirectory(new File(MANAGEMENT_DIR, memberId))
                        .withStorageLevel(StorageLevel.DISK)
                        .build())
                .withPartitionGroups(RaftPartitionGroup
                        .builder(DATA)
                        // 如果不明确指定对应的成员，在出现网络分区时可能造成脑裂
                        .withMembers(memberIds)
                        .withNumPartitions(memberIds.size()-1)
                        .withPartitionSize(memberIds.size()-1)
                        .withDataDirectory(new File(DATA_DIR, memberId))
                        .withStorageLevel(StorageLevel.DISK)
                        .build());

        if (StringUtils.isNotEmpty(groupAddress)) {
            atomixBuilder.withMulticastAddress(new Address(groupAddress, groupPort));
        }
        Atomix atomix = atomixBuilder.build();

        atomix.getCommunicationService().subscribe(WELCOME_CHANNEL, message -> {
            log.info(message.toString());
        }, Executors.newFixedThreadPool(1));

        atomix.getMembershipService().addListener(event -> {
            Member member = event.subject();
            while (!atomix.isRunning()) {
                try {
                    Thread.sleep(500);
                } catch (InterruptedException ignored) {
                }
            }
            switch (event.type()) {
                case MEMBER_ADDED:
                    log.info("Add member : {}", member.toString());
                    if (!member.id().toString().equalsIgnoreCase(memberId)) {
                        // https://atomix.io/docs/latest/user-manual/cluster-communication/direct-messaging/
                        atomix.getCommunicationService().unicast(WELCOME_CHANNEL, memberId + ", say hi!", member.id());
                    }
                    break;
                case MEMBER_REMOVED:
                    log.info("Remove member : {}", member.toString());
                    break;
                case METADATA_CHANGED:
                    log.info("Metadata change : {}", member.toString());
                    break;
                case REACHABILITY_CHANGED:
                    log.info("Reachability change : {}", member.toString());
                    break;
                default:
                    log.warn(event.toString());
                    break;
            }
        });

        return atomix;
    }

    public static void runServer(Atomix atomix) {
        atomix.start().join();
        log.info("Server started");
    }
}
