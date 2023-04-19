import digitalocean
from os import system
from tqdm import tqdm
from time import sleep
from pathlib import Path
from sys import argv


def main_loop(i, key, manager):
    # Spool up 10 VMs
    ssh_keys = manager.get_all_sshkeys()
    droplets = []
    print("Creating droplets...")
    for j in tqdm(range(10)):
        droplet = digitalocean.Droplet(
            token=key,
            name=f"{i+j}",
            region="sfo2",
            image="44080502",
            size_slug="1gb",
            ssh_keys=ssh_keys,
            backups=False,
        )
        droplet.create()
        droplets.append((i + j, droplet))
    print("All droplets created. Waiting for them to be initialised...")

    # Wait until they are all created
    finished = False
    while not finished:
        finished = True
        for k, droplet in droplets:
            actions = droplet.get_actions()
            for action in actions:
                action.load()
                if action.status == "in-progress":
                    finished = False
            if not finished:
                print("Droplets are stil being created. Waiting...")
                sleep(30)
                break
    print("All droplets created! Re-obtaining droplet information...")

    allvalid = False
    while not allvalid:
        drops = manager.get_all_droplets()
        allvalid = True
        for drop in drops:
            if drop.ip_address is None:
                allvalid = False
                print("Still some invalid ips. Retrying...")
                break
            # else:
            #     print(f'Drop ip address is {drop.ip_address}')

    assert len(drops) == len(droplets)
    droplets = []
    for j, drop in enumerate(drops):
        droplets.append((i + j, drop))

    print("Droplet information obtained. Spinning off tasks...")

    # Start the task running
    for k, droplet in tqdm(droplets):
        cmd = f'ssh -o "StrictHostKeyChecking no" root@{droplet.ip_address} bash /root/run.sh {k} > /dev/null &'
        # print(cmd)
        system(cmd)
    print("All tasks started. Waiting 5 minutes...")

    # Wait 5 minutes
    sleep(5 * 60)

    # Collect results
    finished = False
    todestroy = []
    while not finished:
        print("Checking if results are complete...")
        finished = True
        for k, droplet in droplets:
            if droplet not in todestroy:
                failure = system(
                    f'scp -o "StrictHostKeyChecking no" root@{droplet.ip_address}:~/yelpscraper/scrape/results_{k}.tar.gz results/'
                )
                if failure and len(todestroy) < 9:
                    print("Not all results are complete. Sleeping for 1 minute...")
                    sleep(60)
                    finished = False
                    continue
                elif failure:
                    print("Couldn't get one of them, but the rest have finished fine.")
                    break
                else:
                    todestroy.append(droplet)

    print("All droplets complete! Destroying droplets...")
    try:
        for droplet in tqdm(manager.get_all_droplets()):
            droplet.destroy()
        sleep(60)
    except:
        sleep(3 * 60)
        for droplet in tqdm(manager.get_all_droplets()):
            droplet.destroy()

    print("All droplets should be destroyed. Going to next loop...")


if __name__ == "__main__":
    key = ""
    manager = digitalocean.Manager(token=key)
    if len(argv) > 1:
        start = argv[1]
    else:
        start = 0
    for i in tqdm(range(int(start), 60, 10)):
        try:
            main_loop(i, key, manager)
        except:
            sleep(3 * 60)
            try:
                for droplet in tqdm(manager.get_all_droplets()):
                    droplet.destroy()
            except:
                sleep(3 * 60)
            try:
                main_loop(i, key, manager)
            except:
                continue
