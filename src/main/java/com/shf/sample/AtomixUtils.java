package com.shf.sample;

import io.atomix.cluster.Member;
import io.atomix.core.Atomix;

import java.util.Set;

/**
 * description :
 *
 * @author songhaifeng
 * @date 2020/12/10 13:45
 */
public final class AtomixUtils {

    public static Set<Member> getMembers(Atomix atomix) {
        return atomix.getMembershipService().getMembers();
    }

    public static Member getMemberById(Atomix atomix, String memberId) {
        return atomix.getMembershipService().getMember(memberId);
    }
}
