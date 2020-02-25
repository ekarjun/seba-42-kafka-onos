package org.opencord.kafka.integrations;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.onlab.packet.IpAddress;
import org.opencord.cordmcast.CordMcastStatistics;
import org.opencord.cordmcast.CordMcastStatisticsEvent;
import org.opencord.cordmcast.CordMcastStatisticsEventListener;
import org.opencord.cordmcast.CordMcastStatisticsService;
import org.opencord.kafka.EventBusService;
import org.osgi.service.component.annotations.ReferenceCardinality;
import org.osgi.service.component.annotations.Component;
import org.osgi.service.component.annotations.Reference;
import org.osgi.service.component.annotations.ReferencePolicy;
import org.osgi.service.component.annotations.Activate;
import org.osgi.service.component.annotations.Deactivate;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

@Component(immediate = true)
public class McastKafkaIntegration extends AbstractKafkaIntegration {

    @Reference(cardinality = ReferenceCardinality.MANDATORY)
    protected EventBusService eventBusService;

    @Reference(cardinality = ReferenceCardinality.OPTIONAL,
            policy = ReferencePolicy.DYNAMIC,
            bind = "bindMcastStatisticsService",
            unbind = "unbindMcastStatisticsService")
    protected volatile CordMcastStatisticsService ignore1;
    protected final AtomicReference<CordMcastStatisticsService> cordMcastStatisticsServiceRef = new AtomicReference<>();

    private final CordMcastStatisticsEventListener cordMcastStatisticsEventListener =
            new InternalCorcMcastStatisticsListener();

    private static final String MCAST_OPERATIONAL_STATUS_TOPIC = "mcastOperationalStatus.events";

    //cord mcast stats event params
    private static final String GROUP = "group";
    private static final String SOURCE = "source";
    private static final String VLAN = "vlan";

    protected void bindMcastStatisticsService(CordMcastStatisticsService cordMcastStatisticsService) {
        bindAndAddListener(cordMcastStatisticsService, cordMcastStatisticsServiceRef, cordMcastStatisticsEventListener);
    }

    protected void unbindMcastStatisticsService(CordMcastStatisticsService cordMcastStatisticsService) {
        unbindAndRemoveListener(cordMcastStatisticsService,
                cordMcastStatisticsServiceRef, cordMcastStatisticsEventListener);
    }

    @Activate
    public void activate() {
        log.info("Started AaaKafkaIntegration");
    }

    @Deactivate
    public void deactivate() {
        unbindMcastStatisticsService(cordMcastStatisticsServiceRef.get());
        log.info("Stopped AaaKafkaIntegration");
    }

    private void handleMcastStat(CordMcastStatisticsEvent mcastStatEvent) {
        eventBusService.send(MCAST_OPERATIONAL_STATUS_TOPIC, serializeMcastStat(mcastStatEvent));
        log.info("CordMcastStatisticsEvent sent successfully");
    }

    private JsonNode serializeMcastStat(CordMcastStatisticsEvent mcastStatEvent) {
        log.info("Serializing AuthenticationStatisticsEvent");
        ObjectMapper mapper = new ObjectMapper();
        ObjectNode mcastStat = mapper.createObjectNode();
        Map<IpAddress, CordMcastStatistics> subject = mcastStatEvent.subject();
        HashMap<IpAddress, CordMcastStatistics> map = (HashMap<IpAddress, CordMcastStatistics>) subject;
        map.forEach((k, v) -> {
            log.info("Group: " + k + " | Source: " + v.getSourceAddress() + " | Vlan: " + v.getVlanId().toString());
            mcastStat.put("Group: " + k.toString(),
                    " | Source: " + v.getSourceAddress().toString() + " | vlan: " + v.getVlanId().toString());
        });

        return mcastStat;
    }

    private class InternalCorcMcastStatisticsListener implements CordMcastStatisticsEventListener {

        @Override
        public void event(CordMcastStatisticsEvent mcastStatEvent) {
            handleMcastStat(mcastStatEvent);
        }
    }
}
