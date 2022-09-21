hosts=(ssbd-2 ssbd-3 ssbd-4)
images=(hare1039/ssbd:0.0.1)

for i in "${images[@]}"; do
    for h in "${hosts[@]}"; do
        docker save "$i" | pv | ssh "$h" docker load &
    done
    wait < <(jobs -p);
done
