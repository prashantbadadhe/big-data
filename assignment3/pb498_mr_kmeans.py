import sys

from random import uniform
from mrjob.job import MRJob

class KMeans(MRJob):

    def configure_args(self):
        super(KMeans, self).configure_args()
        self.add_file_arg('--centroids', help='Path to centroid.data')
        self.add_passthru_arg('--K', type = int, default = 3, help='Number of centroids')

    def calcDist(self, vec1, vec2):
        temp = (vec1[0] - vec2[0])**2 + (vec1[1] - vec2[1])**2

        return temp **0.5

    def get_centroids(self):
        centroid = open('G:/centroid.data').readlines()
        centroid = [line.strip().split() for line in centroid]
        centroid = [list(map(float, line)) for line in centroid]

        return centroid

    def mapper(self, _, lines):

        centroid = self.get_centroids()

        for line in lines.split('\n'):
            x, y = line.split('\t')
            point = [float(x), float(y)]

            min_dist = sys.maxsize
            label = 0

            for i, cntrd in enumerate(centroid):
                dist = self.calcDist(point, cntrd)

                if dist < min_dist:
                    min_dist = dist
                    label = i
        yield label, point

    def reducer(self, keys, values):

        cnt = 0
        mean_x, mean_y = 0, 0

        for val in values:
            cnt += 1
            mean_x += val[0]
            mean_y += val[1]

        yield keys, (mean_x/cnt, mean_y/cnt)

if __name__ == '__main__':
    KMeans.run()
