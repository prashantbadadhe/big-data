import random

wrtFile = open('input.data', 'w')
cntrdFile = open('centroid.data', 'w')
for _ in range(100):
    wrtFile.writelines('{}\t{}\n'.format(random.randint(-100, 100), random.randint(-100, 100)))

for _ in range(3):
    cntrdFile.writelines('{}\t{}\n'.format(random.randint(-100, 100), random.randint(-100, 100)))
