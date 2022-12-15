import numpy as np
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation
import seaborn as sns
import scipy

fig = plt.figure(1)

ax = fig.subplots(1, 1)

room_shape = (100, 100)

t0 = 0
T = 100000
resolution = 0.007
k = 0.025
t_points = np.linspace(t0, T, int(resolution * T))

t_oven_stop = T / 3
oven_temperature = 80
oven_heat_dissipation_factor = 0.05

initial_temperatures = np.full(room_shape, 10)

# Placement of oven in room
oven_ys, oven_xs = np.mgrid[10:20, 20:40]
initial_temperatures[oven_ys, oven_xs] = oven_temperature


def heat_dissipation(t, y):
    # Current temperatures
    u = y.reshape(room_shape)

    dy, dx = np.gradient(u)

    dydy, _ = np.gradient(dy)
    _, dxdx = np.gradient(dx)

    # Heat equation
    dudu = k * (dydy + dxdx)

    # Edges have a "ghost" point by simply changing as much as their closest
    # inner point
    dudu[[0, -1], :] = dudu[[1, -2], :]
    dudu[:, [0, -1]] = dudu[:, [1, -2]]

    # Dirichlet boundary conditions (edges have constant temperature)
    # dudu[[0, -1], :] = 0
    # dudu[:, [0, -1]] = 0

    # Neumann boundary conditions (edges have constant change of temperature)
    # dudu[0, :] = -0.01 * k

    # Oven condition, constant temperature at oven position until t >
    # t_oven_stop, then gradual decline
    if t < t_oven_stop:
        dudu[oven_ys, oven_xs] = 0
    else:
        dudu[oven_ys, oven_xs] = oven_heat_dissipation_factor * dudu[oven_ys, oven_xs]

    return dudu.reshape(-1)


def steady_state(t, y):
    # Expensive but only way to determine if we approached the "steady state" of
    # the equation
    dudu = heat_dissipation(t, y)
    return np.mean(np.abs(dudu)) - 0.01


steady_state.terminal = True

sol = scipy.integrate.solve_ivp(
    fun=heat_dissipation,
    t_span=(t0, T),
    y0=initial_temperatures.reshape(-1),
    t_eval=t_points,
    # events=(steady_state,),
)

temperatures_over_time = sol.y.T.reshape((-1, *room_shape))
times = sol.t

ax.axis("off")
ln = ax.imshow(temperatures_over_time[0], cmap="turbo")
tn = ax.text(
    0.05,
    0.95,
    f"t: {times[0]:.1f}",
    horizontalalignment="left",
    verticalalignment="top",
    transform=ax.transAxes,
    bbox=dict(
        boxstyle="square",
        ec="k",
        fc="w",
    ),
)
cb = fig.colorbar(ln)


def animate(i):
    ln.set_data(temperatures_over_time[i])
    tn.set_text(f"t: {times[i]:.1f}")

    return ln, tn


if sol.success:
    anim = FuncAnimation(
        fig,
        animate,
        frames=len(temperatures_over_time),
        interval=10,
        repeat=True,
        blit=True,
    )
    ax.set_title("Heat from oven in static air room")

    plt.show()
